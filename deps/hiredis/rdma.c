/**
 * @file rdma.c
 * @author maoxiangyu
 * @brief redis-client with rdma-enabled communication
 *        1. handle connection establishment
 *        2. handle redis-client network I/O
 * @version 0.1
 * @date 2021-12-27
 * 
 * @copyright Copyright (c) 2021
 * 
 */
#include "fmacros.h" //for struct addrinfo declaration
#include "async.h"
#include "async_private.h"
#include "hiredis.h"
#include "rdma.h"
#include <errno.h>


#define UNUSED(V) ((void) V)
void __redisSetError(redisContext *c, int type, const char *str);

#ifdef USE_RDMA
#ifdef __linux__
#define __USE_MISC

#include <endian.h>
#include <netdb.h>
#include <poll.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits.h>
#include <infiniband/verbs.h>
#include <assert.h>
#include <rdma/rdma_cma.h>

#define REDIS_MAX_SGE 1024
#define MIN(a, b)  \
    (a) < (b) ? a : b

typedef enum rdmaOpCode{
    REGISTER_LOCAL_ADDR
} rdmaOpCode;

typedef struct rdmaCommand{ 
    uint8_t used;
    uint8_t version;
    uint8_t opcode;
    uint8_t rsvd[5];
    uint64_t addr;
    uint32_t rkey;
    uint32_t length;
}rdmaCommand;

typedef struct rdmaContext
{
    struct ibv_context *verb_ctx;
    struct ibv_pd *pd;
    struct rdma_event_channel *conn_channel;
    /* attributes for connection identification */
    struct rdma_cm_id *cmid;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;

    /*
     * members related to send buffer at this node
     * send length should be modified when 
     */
    char *send_buffer;
    uint32_t send_length;
    struct ibv_mr *send_region;
    /*
     * members related to buffer processing of remote peer node 
     * avoid transmit too fast to overwrite content of peer node
     * if tx_offset equals tx_length, then give up transmition this 
     * time until peer node reset its recv_offset and rx_offset 
     */
    char *remote_buffer;
    uint32_t remote_key;
    uint32_t tx_offset;
    uint32_t tx_length; 
    
    /*
     * members related to receive buffer at this node
     * when recv_offset euquals recv_length, reset its recv_offset and rx_offset
     */ 
    char *recv_buffer;
    uint32_t recv_length; //total size of receive mr
    uint32_t recv_offset; //next reading position to load into query_buf
    uint32_t rx_offset; // recv_offset equals rx_offset when read from rdma recv buffer
    struct ibv_mr *recv_region;

    /*
     * space to store commands
     * 0 ~ REDIS_MAX_SGE-1 for post recv
     * REDIS_MAX_SGE ~ 2 * REDIS_MAX_SGE - 1 for post send.
     */
    rdmaCommand *cmd_buf;
    struct ibv_mr *cmd_region;
    /* selective signaled */
    uint32_t send_ops;
}rdmaContext;

/* forward declaration */
redisContextFuncs redisContextRdmaFuncs;

int redisContextTimeoutMsec(redisContext *c, long *result);
int redisContextUpdateConnectTimeout(redisContext *c, const struct timeval *timeout);
int redisSetFdBlocking(redisContext *c, int fd, int blocking);

//此处的-999是为了防止计算msec时，结果溢出
#define __MAX_SEC  (((LONG_MAX)-999)/1000)
#define RDMA_DEFAULT_RECV_LEN (1024*1024)
/* 将struct timeval转换为long表示的msec */
static int redisCommandTimeoutMsec(redisContext *c, long *result){
    const struct timeval *timeout = c->command_timeout;
    long msec = INT32_MAX;

    if( timeout != NULL ){
        if (timeout->tv_usec >= 1000000 || (timeout->tv_sec > __MAX_SEC) ){
            *result = msec;
            return REDIS_ERR;
        }

        msec = (timeout->tv_sec * 1000) + ((timeout->tv_usec + 999) / 1000);
        if (msec < 0 || msec > INT32_MAX){
            msec = INT32_MAX;
        }
    }

    *result = msec;
    return REDIS_OK;
}

static inline long redisNow(){
    struct timeval tval;
    if(gettimeofday(&tval, NULL) < 0)
        return -1;
    return tval.tv_sec * 1000 + (tval.tv_usec+999)/1000;
}

/*
 * resource creation and necessary operation before rdma communication
 * memory region creation and error handling
 * 
 */ 
static int rdmaPostRecv( rdmaContext *ctx, struct rdma_cm_id *cmid, rdmaCommand *cmd ){
    int ret = REDIS_OK;
    struct ibv_recv_wr wr, *bad_wr;
    struct ibv_sge sgl;
    memset(&wr, 0, sizeof(wr));
    /* each node maintain a command buffer which used to receive comnmand */
    sgl.addr = (uint64_t)cmd;
    sgl.length = sizeof(rdmaCommand);
    sgl.lkey = ctx->cmd_region->lkey;

    wr.num_sge = 1;
    wr.wr_id = (uint64_t)(cmd);
    wr.sg_list = &sgl;
    wr.next = NULL;

    if( ibv_post_recv(cmid->qp, &wr, &bad_wr) ){
        ret = REDIS_ERR;
    }
    return ret;
}

static void rdmaDestroyIOBuffer(rdmaContext *ctx){
    if(ctx->send_region){
        if(ibv_dereg_mr(ctx->send_region)){
            printf("Redis client deregister send memory region failed:%s\n", strerror(errno));
        }
        ctx->send_region = NULL;
    }
    if(ctx->recv_region){
        if( ibv_dereg_mr(ctx->recv_region)){
            printf("Redis client deregister recv memory region failed:%s\n", strerror(errno));
        }
        ctx->recv_region = NULL;
    }
    if(ctx->cmd_region){
        if( ibv_dereg_mr(ctx->cmd_region)){
            printf("Redis client deregister cmd memory region failed:%s\n", strerror(errno));
        }
        ctx->cmd_region = NULL;
    }
    hi_free(ctx->send_buffer);
    ctx->send_buffer = NULL;
    hi_free(ctx->recv_buffer);
    ctx->recv_buffer = NULL;
    hi_free(ctx->cmd_buf);
    ctx->cmd_buf = NULL;
}

static int rdmaSetupIOBuffer(redisContext *c, rdmaContext *ctx, struct rdma_cm_id *cmid){
    int access_flags = 0;
    access_flags = IBV_ACCESS_LOCAL_WRITE;
    int cmd_len = REDIS_MAX_SGE * 2 * sizeof(rdmaCommand);
    ctx->cmd_buf = (rdmaCommand *)hi_calloc(1, cmd_len);
    ctx->cmd_region = ibv_reg_mr(ctx->pd, ctx->cmd_buf, cmd_len,access_flags);
    if( !ctx->cmd_region ){
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client register command region failed");
        goto destroy_buffer;
    }
    for( int i = 0; i < REDIS_MAX_SGE; i++ ){
        rdmaCommand *cmd = ctx->cmd_buf + i;
        if(rdmaPostRecv(ctx, cmid, cmd)){
           __redisSetError(c, REDIS_ERR_OTHER, "Redis client post recv failed");
           goto destroy_buffer;
        }
    }
    
    ctx->recv_length = RDMA_DEFAULT_RECV_LEN;
    ctx->recv_buffer = hi_calloc(1, ctx->recv_length);
    access_flags = (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    ctx->recv_region = ibv_reg_mr(ctx->pd, ctx->recv_buffer, ctx->recv_length, access_flags);
    if( !ctx->recv_region ){
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client register recv region failed");
        goto destroy_buffer;
    }
    return REDIS_OK;

destroy_buffer:
    rdmaDestroyIOBuffer(ctx);
    return REDIS_ERR;
}

static int rdmaAdjustSendBuffer(rdmaContext *ctx, int length ){
    int ret = REDIS_OK, cur_length = ctx->send_length, access_flags = 0;
    if(length == cur_length){ 
        return ret;
    }
    access_flags = (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if(ibv_dereg_mr(ctx->send_region)){
        ret = REDIS_ERR;
    }
    hi_free(ctx->send_buffer);
    ctx->send_buffer = NULL;
    ctx->send_length = 0;
    if( ret == REDIS_ERR )
        return ret;

    ctx->send_length = length;
    ctx->send_buffer = hi_calloc(1, length);
    ctx->send_region = ibv_reg_mr(ctx->pd, ctx->send_buffer,
                                  length, access_flags);
    if( !ctx->send_region ){
        hi_free(ctx->send_buffer);
        ctx->send_length = 0;
        ctx->send_buffer = NULL;
        ret = REDIS_ERR;
    }
    return ret;
}

/*
 *  redis command write and reply read I/O based on ib verbs
 *  handle event on completion queue(completion for recv and write)
 *  read and write within specified timeout
 */ 
static int rdmaHandleRecv( rdmaContext *ctx, rdmaCommand *cmd ){
    int ret = REDIS_OK;
    uint64_t addr = be64toh(cmd->addr);
    uint32_t rkey = ntohl(cmd->rkey);
    uint32_t length = ntohl(cmd->length);

    switch (cmd->opcode)
    {
    case REGISTER_LOCAL_ADDR:
        ctx->remote_buffer = (char *)addr;
        ctx->remote_key = rkey;
        ctx->tx_offset = 0;
        ctx->tx_length = length;
        if(rdmaAdjustSendBuffer(ctx, ctx->tx_length))
            ret = REDIS_ERR;
        break;
    
    default:
        ret = REDIS_ERR;
        break;
    }
    return ret;
}

static void rdmaHandleSend( rdmaCommand *cmd ){
    cmd->used = 0;
}
static int rdmaHandleWrite( int byte_len ){
    UNUSED(byte_len);
    return REDIS_OK;
}
static int rdmaHandleRecvIMM(rdmaContext *ctx,rdmaCommand *cmd, int offset){
    assert(ctx->rx_offset + offset <= ctx->recv_length);
    ctx->rx_offset +=offset;
    return rdmaPostRecv(ctx, ctx->cmid, cmd);
}

//handle cq event
static int rdmaHandleCq(rdmaContext *ctx){
    int ret = REDIS_OK;

    rdmaCommand *cmd = NULL;
    struct ibv_comp_channel *comp_channel = ctx->comp_channel;
    struct ibv_cq *cq = ctx->cq;

    ret = ibv_get_cq_event(comp_channel, &cq, NULL );
    //above operation is block, so it's necessary to check EAGAIN errno.
    if( ret ){
        if( errno != EAGAIN ){
            ret = REDIS_ERR;
            return ret;
        }
    }
    if( ibv_req_notify_cq(cq, 0) ){
        ret = REDIS_ERR;
        return ret;
    }

    struct ibv_wc wc[REDIS_MAX_SGE * 2];

    int cq_events = 0;
    cq_events = ibv_poll_cq(cq, REDIS_MAX_SGE*2, wc);
    if( cq_events == 0 ){
        return ret;   
    }
    ibv_ack_cq_events(cq, cq_events);
    for( int i = 0; i < cq_events; i++ ){
        switch (wc[i].opcode){
        case IBV_WC_RECV_RDMA_WITH_IMM:
            cmd = (rdmaCommand *)(wc[i].wr_id);
            if (rdmaHandleRecvIMM(ctx, cmd, wc[i].byte_len) == REDIS_ERR){
                return REDIS_ERR;
            }
            break;

        case IBV_WC_RECV:
            cmd = (rdmaCommand *)(wc[i].wr_id);
            if (rdmaHandleRecv(ctx, cmd) == REDIS_ERR ){
                return REDIS_ERR;
            }
            break;
        case IBV_WC_SEND:
            cmd = (rdmaCommand *)(wc[i].wr_id);
            rdmaHandleSend(cmd);
            break;
        case IBV_WC_RDMA_WRITE:
            rdmaHandleWrite(wc[i].byte_len);
            break;
        default:
            break;
        }
    }
    return ret;
} 

static int rdmaSendCommand( rdmaContext *ctx,struct rdma_cm_id *cmid, rdmaCommand *cmd ){
    int ret = REDIS_OK;
    rdmaCommand *send_cmd;
    struct ibv_send_wr wr, *bad_wr;
    struct ibv_sge sgl;
    memset(&wr, 0, sizeof(wr));

    for (int i = REDIS_MAX_SGE; i < 2 * REDIS_MAX_SGE; i++){
        send_cmd = ctx->cmd_buf + i;
        if (!send_cmd->used){
            send_cmd->used = 1;
            break;
        }
    }

    send_cmd->addr = htobe64(cmd->addr);
    send_cmd->length = htonl(cmd->length);
    send_cmd->rkey = htonl(cmd->rkey);

    send_cmd->opcode = REGISTER_LOCAL_ADDR;
    send_cmd->used = 1;
    send_cmd->version = 1;

    sgl.addr = (uint64_t)send_cmd;
    sgl.length = sizeof(rdmaCommand);
    
    wr.num_sge = 1;
    wr.sg_list = &sgl;
    wr.send_flags |= IBV_SEND_INLINE;
    wr.send_flags |= IBV_SEND_SIGNALED;
    wr.opcode = IBV_WR_SEND;
    wr.next = NULL;
    wr.wr_id = (uint64_t)(&send_cmd);

    ret = ibv_post_send(cmid->qp, &wr, &bad_wr);
    if( ret ){
        ret = REDIS_ERR;
    }else
        ret = REDIS_OK;
    return ret;
}
/* synchronize receive buffer of current node to the remote node */
static int syncRecvBuffer( rdmaContext *ctx, struct rdma_cm_id *cmid){
    rdmaCommand cmd;
 
    cmd.addr = (uint64_t)ctx->recv_buffer;
    cmd.length = ctx->recv_length;
    cmd.rkey = ctx->recv_region->rkey;

    ctx->recv_offset = 0;
    ctx->rx_offset = 0;
 
    return rdmaSendCommand(ctx, cmid, &cmd);
}

/* 
 * Implementation of redisContextFuncs for RDMA connections
 * redis client consume data from receive buffer filled by
 * rdma write(with IMM) of redis server.
 * return total number of reads or REDIS_ERR when timeout
 */
static void redisRdmaFree(void *privctx){
    if( !privctx )
        return;

    rdmaContext *ctx = (rdmaContext *)(privctx);
    if(ibv_destroy_cq(ctx->cq))
        printf("Redis client destroy completion queue failed:%s\n", strerror(errno));
    rdmaDestroyIOBuffer(ctx);
    rdma_destroy_event_channel(ctx->conn_channel);
    if (ibv_destroy_comp_channel(ctx->comp_channel))
        printf("Redis client destroy completion channel failed:%s\n", strerror(errno));
    ibv_dealloc_pd(ctx->pd);
    hi_free(ctx);
}

static void redisRdmaClose( redisContext *c ){
    struct rdmaContext *ctx = (rdmaContext *)(c->privctx);
    struct rdma_cm_id *cmid = ctx->cmid;
    rdma_disconnect(cmid);
    rdmaHandleCq(ctx);
    redisRdmaFree(ctx);
    c->privctx = NULL;
    if(ibv_destroy_qp(cmid->qp))
        printf("Redis client destroy qp failed:%s\n", strerror(errno));
    if(rdma_destroy_id(cmid))
        printf("Redis client destroy cmid failed:%s\n", strerror(errno));
    /*ibv_destroy_cq(ctx->cq);
    ibv_destroy_qp(cmid->qp);
    rdmaDestroyIOBuffer(ctx);
    if(rdma_destroy_id(cmid))
        printf("Redis client destroy cmid failed:%s", strerror(errno));
    rdma_destroy_event_channel(ctx->conn_channel);
    ibv_destroy_comp_channel(ctx->comp_channel);
    ibv_dealloc_pd(ctx->pd);*/
}

void redisRdmaAsyncRead(redisAsyncContext *ac) {
     UNUSED(ac);
     assert("hiredis async mechanism can't work with RDMA" == NULL);
 }

 void redisRdmaAsyncWrite(redisAsyncContext *ac) {
     UNUSED(ac);
     assert("hiredis async mechanism can't work with RDMA" == NULL);
 }
 
ssize_t redisRdmaRead(redisContext *c, char *buf, size_t buflen){
    rdmaContext *ctx = (rdmaContext *)(c->privctx);
    long timeout = 0;
    long start = redisNow();

    if (redisCommandTimeoutMsec(c, &timeout) == REDIS_ERR)
        return REDIS_ERR;
    size_t avail;
    while ( 1 ){
        if( redisNow() - start > timeout )
            break;
        if(rdmaHandleCq(ctx) == REDIS_ERR){
            return REDIS_ERR;
        }
        avail = ctx->rx_offset - ctx->recv_offset;
        if (avail > 0)
            break;
        struct pollfd fds[1];
        fds[0].fd = ctx->comp_channel->fd;
        fds[0].events = POLLIN;
        if (poll(fds, 1, 1000) == -1){
            return REDIS_ERR;
        }
    }
    if( avail == 0 ){
        __redisSetError(c, REDIS_ERR_TIMEOUT, "Redis client read timeout");
        return REDIS_ERR;
    }

    ssize_t nread = MIN(avail, buflen);
    memcpy(buf, ctx->recv_buffer+ctx->recv_offset, nread);
    ctx->recv_offset +=nread;
    
    if(ctx->recv_offset == ctx->recv_length)
        syncRecvBuffer(ctx, ctx->cmid);
    return nread;


/*
read_data:

    size_t avail = ctx->rx_offset - ctx->recv_offset;

    ssize_t nread = MIN(avail, buflen);
    memcpy(buf, ctx->recv_buffer + ctx->recv_offset, nread);
    ctx->recv_offset += nread;
    if (ctx->recv_offset == ctx->recv_length)
        syncRecvBuffer(ctx, ctx->cmid);
    return nread;

wait_data:
    if (rdmaHandleCq(ctx) == REDIS_ERR)
        return REDIS_ERR;

    if (ctx->rx_offset > ctx->recv_offset)
        goto read_data;
    struct pollfd fds[1];
    fds[0].fd = ctx->comp_channel->fd;
    fds[0].events = POLLIN;
    if (poll(fds, 1, 1000) == -1){
        return REDIS_ERR;
    }
    if (redisNow() - start <= timeout)
        goto read_data;
*/
       
}

static int ibVerbsWrite(rdmaContext *ctx, size_t write_len){

    int ret = 0;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    uint32_t offset = ctx->tx_offset;
    struct rdma_cm_id *cmid = ctx->cmid;
    memset(&wr, 0, sizeof(wr));

    sge.addr = (uint64_t)(ctx->send_buffer + offset);
    sge.length = write_len;
    sge.lkey = ctx->send_region->lkey;
    wr.num_sge = 1;
    wr.sg_list = &sge;
    wr.next = NULL;

    wr.wr_id = 0;
    wr.imm_data = htonl(0);

    wr.wr.rdma.remote_addr = (uint64_t)ctx->remote_buffer + offset;
    wr.wr.rdma.rkey = ctx->remote_key;
    
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    //wr.send_flags |= IBV_SEND_SIGNALED; 
    //selective signaled
    wr.send_flags = ((++ctx->send_ops) % REDIS_MAX_SGE) ? 0 : IBV_SEND_SIGNALED; 

    ret = ibv_post_send( cmid->qp, &wr, &bad_wr );
    if( ret ){
        return ret;
    }
    ctx->tx_offset +=write_len;

    return write_len;
}
/*
 * redis client write data from send buffer to 
 * receive buffer of redis server via rdma write
 * return total amount of written data 
 * or REDIS_ERR if timeout or rdma write errors
 */ 
ssize_t redisRdmaWrite(redisContext *c){
    rdmaContext *ctx = (rdmaContext *)(c->privctx);
    size_t data_len = hi_sdslen(c->obuf);
    size_t totalwrite = 0;
    long timeout = 0;
    struct pollfd rfd[1];
    rfd[0].fd = ctx->comp_channel->fd;
    rfd[0].events = POLLIN;

    if( redisCommandTimeoutMsec(c, &timeout) ==REDIS_ERR )
        return REDIS_ERR;

    size_t avail = ctx->tx_length - ctx->tx_offset;
    long start = redisNow();
    while ( 1 ){
        while (avail == 0){
            if( redisNow() - start > timeout )
                goto external_loop;
            // waiting for 1 milliseconds for syncRecvBuffer called by server
            if (poll(rfd, 1, 1) < 0){
                return REDIS_ERR;
            }
            if (rdmaHandleCq(ctx))
                return REDIS_ERR;
            avail = ctx->tx_length - ctx->tx_offset;
        }
        int nwritten = MIN(avail, data_len - totalwrite);
        memcpy(ctx->send_buffer + ctx->tx_offset, c->obuf + totalwrite, nwritten);
        int ret = ibVerbsWrite(ctx, nwritten);
        //!!!write error occured, return immediately
        if (ret < 0)
            return REDIS_ERR;
        totalwrite += nwritten;
        if (totalwrite == data_len)
            break;
        avail = ctx->tx_length - ctx->tx_offset;
    }
    external_loop:
    if( totalwrite == 0 ){
        __redisSetError(c, REDIS_ERR_TIMEOUT, "Redis client write data timeout");
        return REDIS_ERR;
    }
    return totalwrite;
}


/* redisContext functions based on RDMA */
redisContextFuncs redisContextRdmaFuncs = {
    .close = redisRdmaClose,
    .free_privctx = redisRdmaFree,
    .async_read = redisRdmaAsyncRead,
    .async_write = redisRdmaAsyncWrite,
    .read = redisRdmaRead,
    .write = redisRdmaWrite
};

/*
 * the code below is related to client rdma connection
 * establishment, including handling connection handshake 
 * before timeout, create rdmaContext specific to this connection
 * transist the redisContext flag into REDIS_CONNECTED
 */ 
static int rdmaHandleConnect(redisContext *c, struct rdma_cm_id *cm_id ){
    rdmaContext *ctx = (rdmaContext *)(c->privctx);
    struct ibv_pd *pd;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;

    //ctx注入进cm_id中
    cm_id->context = ctx;

    pd = ibv_alloc_pd(cm_id->verbs);
    if( !pd ){
        printf("Redis client allocate protection domain failed\n");
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client allocate protection domain failed");
        goto freeresources;
    }
    ctx->pd = pd;
    comp_channel = ibv_create_comp_channel(cm_id->verbs);
    if( !comp_channel ){
        printf("Redis client create comp channel failed\n");
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client create completion channel failed");
        goto freeresources;
    }
    ctx->comp_channel = comp_channel;
    cq = ibv_create_cq(cm_id->verbs, REDIS_MAX_SGE * 2, NULL, comp_channel, 0);
    if( !cq ){
        printf("Redis client create completion queue failed\n");
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client create completion queue failed");
        goto freeresources;
    }
    ctx->cq = cq;
    // second parameter: solicited only(only notify if WR is flagged as solicited)
    if(ibv_req_notify_cq(cq,0)){
        printf("Redis client arm notification for cq failed:%s\n", strerror(errno));
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client arm notification for cq failed");
        goto freeresources;
    } 

    //初始化cm_id对应的queue pair
    struct ibv_qp_init_attr qp_init_attr;
    memset( &qp_init_attr, 0, sizeof(qp_init_attr) );
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.recv_cq = cq;
    qp_init_attr.send_cq = cq;
    qp_init_attr.cap.max_send_wr = REDIS_MAX_SGE;
    qp_init_attr.cap.max_recv_wr = REDIS_MAX_SGE;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    if(rdma_create_qp(cm_id, pd, &qp_init_attr)){
        printf("Redis client create qp failed:%s", strerror(errno));
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client create qp for rdma transport failed");
        goto freeresources;
    }

    //set up memory region
    if(rdmaSetupIOBuffer(c, ctx, cm_id))
        goto freeqp;

    struct rdma_conn_param cm_params = {
        .responder_resources = 1,
        .initiator_depth = 1,
        .retry_count = 7,
        .rnr_retry_count = 7
    };
    if(rdma_connect(cm_id, &cm_params)){
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client rdma connect failed");
        goto destroybuf;        
    }
    return REDIS_OK;

destroybuf:
    rdmaDestroyIOBuffer(ctx);
freeqp:
    ibv_destroy_qp(cm_id->qp);
freeresources:
    if (ctx->cq)
        ibv_destroy_cq(ctx->cq);
    if (ctx->comp_channel)
        ibv_destroy_comp_channel(ctx->comp_channel);
    if (ctx->pd)
        ibv_dealloc_pd(ctx->pd);

    return REDIS_ERR;
}

static int rdmaHandleEstablished(redisContext *c, struct rdma_cm_id *cmid){
    rdmaContext *ctx = (rdmaContext *)c->privctx;

    c->funcs = &redisContextRdmaFuncs;
    c->fd = ctx->comp_channel->fd;
    c->flags |= REDIS_CONNECTED;
    
    return syncRecvBuffer(ctx, cmid);
}

static int redisRdmaHandleConnect(redisContext *c, int timeout){
    int ret = REDIS_OK;
    char errstr[128];
    rdmaContext *ctx = (rdmaContext *)c->privctx;
    struct rdma_cm_event *cm_event;
    while( rdma_get_cm_event(ctx->conn_channel, &cm_event)==0 ){
        switch (cm_event->event)
        {
        case RDMA_CM_EVENT_ADDR_RESOLVED:
            if (timeout < 0 || timeout > 100)
                timeout = 100;
            ret = rdma_resolve_route(cm_event->id, timeout);
            if (ret){
                ret = REDIS_ERR;
                printf("Redis client rdma resolve route failed:%s\n", strerror(errno));
                __redisSetError(c, REDIS_ERR_OTHER, "Redis client rdma resolve route failed");
            }
            break;

        case RDMA_CM_EVENT_ROUTE_RESOLVED:
            ret = rdmaHandleConnect(c, cm_event->id);
            break;

        case RDMA_CM_EVENT_ESTABLISHED:
            ret = rdmaHandleEstablished(c, cm_event->id);
            break;

        case RDMA_CM_EVENT_TIMEWAIT_EXIT:
            break;

        case RDMA_CM_EVENT_ADDR_ERROR:
        case RDMA_CM_EVENT_ROUTE_ERROR:
        case RDMA_CM_EVENT_CONNECT_ERROR:
        case RDMA_CM_EVENT_UNREACHABLE:
        case RDMA_CM_EVENT_REJECTED:
        case RDMA_CM_EVENT_DISCONNECTED:
        default:
            ret = REDIS_ERR;
            snprintf(errstr, sizeof(errstr), "Redis client error rdma event occured:%s", rdma_event_str(cm_event->event));
            __redisSetError(c, REDIS_ERR_OTHER, errstr);
            break;
        }
        if(rdma_ack_cm_event(cm_event) == -1){
            snprintf(errstr, sizeof(errstr), "Redis client ack connect event failed:%s", rdma_event_str(cm_event->event));
            __redisSetError(c, REDIS_ERR_OTHER, errstr); 
            return REDIS_ERR;
        }
    }
    return ret;
}

static int redisContextWaitReady(redisContext *c, long timeout){
    struct pollfd fds[1];
    rdmaContext *ctx = (rdmaContext *)c->privctx;
    fds[0].fd = ctx->conn_channel->fd;
    fds[0].events = POLLIN;
    long start = redisNow();

    while ( 1 ){
        long remain_time = timeout - (redisNow() - start);
        if( remain_time <= 0 ) {
            break;
        }
        if (poll(fds, 1, remain_time) < 0){
            printf("Redis client poll waiting for handshake completion failed\n");
            return REDIS_ERR;
        }
        if ( redisRdmaHandleConnect(c, remain_time) ==REDIS_ERR ){
            return REDIS_ERR;
        }
        if (c->flags | REDIS_CONNECTED)
            return REDIS_OK;
    }
    return REDIS_ERR;
}

int redisInitiateRdmaConnection(redisContext *c, const char *addr, int port,const struct timeval *timeout){
    int rv = REDIS_OK;
    char _port[6];
    struct addrinfo hints, *clientinfo = NULL, *p;
    long time_remain = -1, start = redisNow();
    struct rdma_cm_id *cmid = NULL;
    rdmaContext *ctx = NULL;
    struct sockaddr_storage sa;
    long total_time = -1;

    c->connection_type = REDIS_CONN_RDMA;
    c->tcp.port = port;

    if (timeout){
        if (redisContextUpdateConnectTimeout(c, timeout) == REDIS_ERR){
            __redisSetError(c, REDIS_ERR_OOM, "Redis client context allocate timeout failed");
            goto error;
        }
    }
    else{
        hi_free(c->connect_timeout);
        c->connect_timeout = NULL;
    }

    if( redisContextTimeoutMsec(c, &total_time) == REDIS_ERR){
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client get timeout failed");
        return REDIS_ERR;
    }else if(total_time == -1){
        total_time = INT32_MAX;
    }

    if (c->tcp.host != addr) {
        hi_free(c->tcp.host);

        c->tcp.host = hi_strdup(addr);
        if (c->tcp.host == NULL){
            __redisSetError(c, REDIS_ERR_OOM, "Redis client Out of memory: allocate host str failed");
            return REDIS_ERR;
        }
    }

    snprintf(_port, 6, "%d", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    /* create rdma event channel and call rdma_resolv_addr, wait specific timeout to call rdmaHandleConnect */
    if( (rv = getaddrinfo(c->tcp.host, _port, &hints, &clientinfo)) != 0){
        hints.ai_family = AF_INET6;
        if((rv=getaddrinfo(addr, _port, &hints, &clientinfo))!=0){
            __redisSetError(c, REDIS_ERR_OTHER, gai_strerror(rv));
            return REDIS_ERR;
        }
    }
    /* create connect event channel, create rdmaContext */
    ctx = hi_calloc(1, sizeof(rdmaContext));
    if( ctx == NULL ){
        __redisSetError(c, REDIS_ERR_OOM, "Redis client allocate rdmaContext structure failed");
        goto error;
    }
    c->privctx = ctx;

    struct rdma_event_channel *channel = rdma_create_event_channel();
    if( channel == NULL ){
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client create cm event channel failed");
        goto error;
    }
    ctx->conn_channel = channel;
    /* set connection event channel non-blocking to change default rdma_get_cm_event(...) synchronouse behavior */
    if((redisSetFdBlocking(c, ctx->conn_channel->fd, 0)) != REDIS_OK){
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client set cm channel fd non-blocking failed");
        goto err_destroy_channel;
    }

    if(rdma_create_id(channel, &cmid,ctx, RDMA_PS_TCP)){
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client rdma cmid create failed");
        goto err_destroy_channel;
    }
    ctx->cmid = cmid;
    for( p = clientinfo; p!=NULL; p = p->ai_next ){
        /* call rdma_resolve_addr */
        memcpy(&sa, p->ai_addr, p->ai_addrlen);
        if(rdma_resolve_addr(ctx->cmid, NULL, (struct sockaddr*)(&sa), 10)){
            continue;
        }
        time_remain = total_time - (redisNow() - start);
        if( time_remain <= 0 ){
           __redisSetError(c, REDIS_ERR_OTHER, "Redis client connection establishment timeout" ); 
           rv = REDIS_ERR;
           goto err_destroy_cmid;
        }
        if( ( redisContextWaitReady(c, time_remain) == REDIS_OK) && (c->flags | REDIS_CONNECTED)){
            rv = REDIS_OK; 
            goto end;
        }
        
    }
    if( (!c->err) && (p == NULL) ){
        __redisSetError(c, REDIS_ERR_OTHER, "Redis client resolve addr failed" );
    }

err_destroy_cmid:
    rdma_destroy_id(ctx->cmid);
err_destroy_channel:
    rdma_destroy_event_channel(ctx->conn_channel);
error:
    rv = REDIS_ERR;
    if(ctx){
        hi_free(ctx);
    }
end:
    if(clientinfo){
        freeaddrinfo(clientinfo);
    }

    return rv;
}

int redisContextConnectRdma(redisContext *c, const char *addr, int port,
                           const struct timeval *timeout){
    return redisInitiateRdmaConnection(c, addr, port, timeout);
}

#else
"Build error, RDMA related library should be run on linux platform"
#endif
#else
int redisInitiateRdmaConnection(redisContext *c, const char *addr, int port, const struct timeval *timeout){
    UNUSED(c);
    UNUSED(addr);
    UNUSED(port);
    UNUSED(timeout);
    __redisSetError(c, REDIS_ERR_OTHER, "Build with RDMA failed, need option declaration BUILD_RDMA=yes");
    return EOPNOTSUPP;
}
int redisContextConnectRdma(redisContext *c, const char *addr, int port,
                           const struct timeval *timeout){
    return redisInitiateRdmaConnection(c, addr, port, timeout);
}
#endif
