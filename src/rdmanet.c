/**
 * @file rdmanet.c
 * @author maoxiangyu (luoxi0598@qq.com)
 * @brief connection related operation of specific RDMA connection type
 * @date 2021-12-03
 * 
 * @copyright Copyright (c) 2021
 */

#include "server.h"
#include "connhelpers.h"
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <stdbool.h>

#define REDIS_MAX_SGE 1024
#define REDIS_SYNC_MS 10
#define MIN(a, b)  \
    (a) < (b) ? a : b

extern struct rdma_event_channel *listen_channel;

typedef struct rdmaConnection rdmaConnection;
typedef struct rdmaContext rdmaContext;

/*
 * data structure needed for memory region related
 * info, used to fill rdmaContext data structure
 */
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

/*
 * rdmaConnection class for each server-client rdma connection
 * analygy to connection class for server-client tcp connection
 */ 
struct rdmaConnection{
    connection conn;
    struct rdma_cm_id *cm_id;
};

/*
 * data structure for each server-client connection
 * each server-client connection maintain a queue pair 
 * this structure is stored in context attribute of the 
 * corresponding cm_id. 
 * 
 * including buffer position pointer to manage buffer.
 */
struct rdmaContext
{
    rdmaConnection *connection;
    struct ibv_context *verb_ctx;
    struct ibv_pd *pd;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;

    long long timeevent;

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
};


ConnectionType CT_rdma;

/* receive queue related operation */
static int rdmaPostRecv(rdmaContext *ctx,struct rdma_cm_id *cmid, rdmaCommand *cmd);
static int rdmaHandleCq(rdmaConnection *conn);
/* memory buffer register and deregister related function */
#define RDMA_DEFAULT_RECV_LEN 1024 * 1024

static void rdmaDestroyIOBuffer(rdmaContext *ctx){
    if(ctx->send_region){
        if(ibv_dereg_mr(ctx->send_region)){
            serverLog(LL_WARNING, "server deregiter send memory region failed:%s", strerror(errno));
        }
        ctx->send_region = NULL;
    }
    if(ctx->recv_region){
        if( ibv_dereg_mr(ctx->recv_region)){
             serverLog(LL_WARNING, "server deregiter recv memory region failed:%s", strerror(errno));
        }
        ctx->recv_region = NULL;
    }
    if(ctx->cmd_region){
        if( ibv_dereg_mr(ctx->cmd_region)){
            serverLog(LL_WARNING, "server deregiter cmd memory region failed:%s", strerror(errno));
        }
        ctx->cmd_region = NULL;
    }
    zfree(ctx->send_buffer);
    ctx->send_buffer = NULL;
    zfree(ctx->recv_buffer);
    ctx->recv_buffer = NULL;
    zfree(ctx->cmd_buf);
    ctx->cmd_buf = NULL;
}

static int rdmaPostRecv( rdmaContext *ctx, struct rdma_cm_id *cmid, rdmaCommand *cmd ){
    int ret = C_OK;

    struct ibv_recv_wr wr, *bad_wr;
    struct ibv_sge *sgl;
    memset(&wr, 0, sizeof(wr));
    /* each node maintain a command buffer which used to receive comnmand */
    sgl->addr = (uint64_t)cmd;
    sgl->length = sizeof(rdmaCommand);
    sgl->lkey = ctx->cmd_region->lkey;

    wr.num_sge = 1;
    wr.wr_id = (uint64_t)(cmd);
    wr.sg_list = &sgl;
    wr.next = NULL;

    if( ibv_post_recv(cmid->qp, &wr, &bad_wr) ){
        ret = C_ERR;
        serverLog(LL_WARNING, "RDMA: post RECV failed:%s", strerror(errno));    
    }
    return ret;
}

static int rdmaSetupIOBuffer(rdmaContext *ctx, struct rdma_cm_id *cmid){
    int access_flags = 0;
    access_flags = IBV_ACCESS_LOCAL_WRITE;
    int cmd_len = REDIS_MAX_SGE * 2 * sizeof(rdmaCommand);
    ctx->cmd_buf = (rdmaCommand *)zcalloc(cmd_len);
    ctx->cmd_region = ibv_reg_mr(ctx->pd, ctx->cmd_buf, cmd_len,access_flags);
    if( !ctx->cmd_region ){
        serverLog(LL_WARNING, "server register command memory region failed");
        goto destroy_buffer;
    }
    for( int i = 0; i < REDIS_MAX_SGE; i++ ){
        rdmaCommand *cmd = ctx->cmd_buf + i;
        if(rdmaPostRecv(ctx, cmid, cmd)){
            serverLog(LL_WARNING,"server post recv failed");
            goto destroy_buffer;
        }
    }
    
    ctx->recv_length = RDMA_DEFAULT_RECV_LEN;
    ctx->recv_buffer = zcalloc(ctx->recv_length);
    access_flags = (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    ctx->recv_region = ibv_reg_mr(ctx->pd, ctx->recv_buffer, ctx->recv_length, access_flags);
    if( !ctx->recv_region ){
        serverLog(LL_WARNING, "server register memory region failed");
        goto destroy_buffer;
    }
    return C_OK;

destroy_buffer:
    rdmaDestroyIOBuffer(ctx);
    return C_ERR;
}

static int rdmaAdjustSendBuffer(rdmaContext *ctx, int length ){
    int ret = C_OK, cur_length = ctx->send_length, access_flags = 0;
    if(length == cur_length){ 
        return ret;
    }
    access_flags = (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if(ibv_dereg_mr(ctx->send_region)){
        serverLog(LL_WARNING, "server deregiter cmd memory region failed:%s", strerror(errno));
        ret = C_ERR;
    }
    zfree(ctx->send_buffer);
    ctx->send_buffer = NULL;
    ctx->send_length = 0;
    if( ret == C_ERR )
        return ret;

    ctx->send_length = length;
    ctx->send_buffer = zcalloc(length);
    ctx->send_region = ibv_reg_mr(ctx->pd, ctx->send_buffer,
                                  length, access_flags);
    if( !ctx->send_region ){
        serverLog(LL_WARNING, "server register memory region failed");
        zfree(ctx->send_buffer);
        ctx->send_length = 0;
        ctx->send_buffer = NULL;
        ret = C_ERR;
    }
    return ret;
}

static int connRdmaWait( connection *c, long long start, long long timeout){
    rdmaConnection *rdma_conn = (rdmaConnection *)(c);
    long avail = MIN(timeout, REDIS_SYNC_MS);
    if(aeWait(c->fd, AE_READABLE, avail) < 0){
        return C_ERR;
    }
    long elapse = mstime() - start;
    if(elapse > avail){
        errno = ETIMEDOUT;
        return C_ERR;
    }
    if( rdmaHandleCq(rdma_conn) == C_ERR ){
        c->state = CONN_STATE_ERROR;
        return C_ERR;
    }
    return C_OK;
}

static int connRdmaConnect(connection *c, const char *addr, int port, const char *src_addr){
    serverLog(LL_WARNING, "Redis server call connect in RDMA context");
    return C_ERR;
}
static int connRdmaBlockingConnect(connection *c, const char *addr, int port, long timeout){
    serverLog(LL_WARNING, "Redis server call blocking connect in RDMA context");
    return C_ERR;
}

/**
 *  1. memory region addr and offset info synchronization 
 *  2. buffer read/write pointer synchronization 
 */
static int rdmaSendCommand( rdmaContext *ctx, struct rdma_cm_id *cmid, rdmaCommand *cmd ){
    int ret = 0;
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

    send_cmd->addr = htonu64(cmd->addr);
    send_cmd->length = htonl(cmd->length);
    send_cmd->rkey = htonl(cmd->rkey);

    send_cmd->opcode = REGISTER_LOCAL_ADDR;
    send_cmd->used = 1;
    send_cmd->version = 1;

    sgl.addr = &send_cmd;
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
        ret = C_ERR;
        serverLog(LL_WARNING, "RDMA: SEND command failed:%s", strerror(errno));
    }
    return ret;
}

static int syncRecvBuffer( rdmaContext *ctx, struct rdma_cm_id *cmid){
    rdmaCommand cmd;
 
    cmd.addr = (uint64_t)ctx->recv_buffer;
    cmd.length = ctx->recv_length;
    cmd.rkey = ctx->recv_region->rkey;

    ctx->recv_offset = 0;
    ctx->rx_offset = 0;
 
    return rdmaSendCommand(ctx, cmid, &cmd);
}

/**
 * network I/O reimplemented in RDMA communication
 */

static int ibVerbsWrite(connection *c, size_t write_len){

    int ret = 0;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    rdmaConnection *rdma_conn = (rdmaConnection *)(c);
    struct rdma_cm_id *cmid = rdma_conn->cm_id;
    rdmaContext *ctx = (rdmaContext *)(cmid->context);

    uint32_t offset = ctx->tx_offset;

    memset(&wr, 0, sizeof(wr));

    sge.addr = (uint64_t)(ctx->send_buffer + offset);
    sge.length = write_len;
    sge.lkey = ctx->send_region->lkey;
    wr.num_sge = 1;
    wr.sg_list = &sge;
    wr.next = NULL;

    wr.wr_id = 0;
    wr.imm_data = htonl(0);

    wr.wr.rdma.remote_addr = ctx->remote_buffer + offset;
    wr.wr.rdma.rkey = ctx->remote_key;
    
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags |= IBV_SEND_SIGNALED; //todo::selective signaled

    ret = ibv_post_send( cmid->qp, &wr, &bad_wr );
    if( ret ){
        rdma_conn->conn.last_errno = errno;
        serverLog(LL_WARNING, "RDMA: WRITE failed:%s", connGetLastError(errno));
        if( rdma_conn->conn.state == CONN_STATE_CONNECTED )
            rdma_conn->conn.state = CONN_STATE_ERROR;
        return ret;
    }
    ctx->tx_offset +=write_len;

    return write_len;
}
/*
 * memcpy [data, data+data_len) into send buffer 
 * ranged [tx_offset,tx_length), and execute RDMA 
 * Write. if return 0, then writeToClient() continues
 * running until tx_offset is reset.
 */ 
static int connRdmaWrite( connection *c, const void *data, size_t data_len ){
    rdmaConnection *rdma_conn = (rdmaConnection *)(c);
    rdmaContext *rdma_ctx = (rdmaContext *)rdma_conn->cm_id->context;

    serverAssert(rdma_ctx->tx_length >= rdma_ctx->tx_offset);
    size_t  remaining = rdma_ctx->tx_length - rdma_ctx->tx_offset;
    size_t nwritten = MIN( data_len, remaining);

    if( nwritten == 0 )
        return 0;
    memcpy(rdma_ctx->send_buffer + rdma_ctx->tx_offset, data, nwritten );    
    return ibVerbsWrite(c, nwritten);
}
/*
 * memcpy recv_buf content of rdmaContext into [buf, buf+buf_len)
 * (i.e query_buf in client structure) until recv_offset equals 
 * rx_offset
 */ 
static int connRdmaRead( connection *c, void *buf, size_t buf_len ){
    rdmaConnection *rdma_conn = (rdmaConnection *)(c);
    rdmaContext *ctx = (rdmaContext *)rdma_conn->cm_id->context;

    serverAssert(ctx->recv_offset < ctx->rx_offset);
    size_t available = ctx->rx_offset - ctx->recv_offset;
    size_t nread = MIN(available, buf_len);

    memcpy(buf, ctx->recv_buffer+ctx->recv_offset, nread );
    ctx->recv_offset +=nread;

    return nread;
}

static ssize_t connRdmaSyncWrite(connection *c, char *ptr, ssize_t size, long long timeout){
    ssize_t nwritten = 0;
    rdmaConnection *rdma_conn = (rdmaConnection *)(c);
    rdmaContext *ctx = (rdmaContext *)rdma_conn->cm_id->context;
    ssize_t avail = ctx->tx_length - ctx->tx_offset;
    long long remain_time = timeout;
    long long start = mstime();
    while( nwritten < size ){
        ssize_t written = MIN(avail, size - nwritten);
        while( written == 0 && remain_time > 0 ){
            if(connRdmaWait(c, start, timeout) == C_ERR){
                serverLog(LL_WARNING, "Redis server wait for available space to sync write timeout");
                return C_ERR;
            }
            written = MIN(ctx->tx_length - ctx->tx_offset, size - nwritten);
            remain_time -=(mstime()-start);
        }
        if( remain_time <= 0 )
            break;
        memcpy(ctx->send_buffer+ctx->tx_offset, ptr, written);
        if( ibVerbsWrite(c, written) == C_ERR ){
            serverLog(LL_WARNING, "Redis server sync write failed");
            return C_ERR;
        }
        nwritten +=written;
        ptr +=written;
        avail = ctx->tx_length - ctx->tx_offset;
        remain_time -=(mstime() - start);
    }
    if( nwritten != size ){
        serverLog(LL_WARNING, "Redis server sync write data tiemout written %d amount of data", nwritten);
        return C_ERR;
    }
    return nwritten;
}

static ssize_t connRdmaSyncRead(connection *c, char *ptr, ssize_t size, long long timeout){
    rdmaConnection *rdma_conn = (rdmaConnection *)(c);
    rdmaContext *ctx = (rdmaContext *)rdma_conn->cm_id->context;

    ssize_t nread = 0;
    long long remain_time = timeout, start = mstime();
    while( nread < size ){
        ssize_t readn = MIN(ctx->rx_offset - ctx->recv_offset, size-nread);
        while (readn == 0 && remain_time > 0){
            if(connRdmaWait(c, start, timeout) == C_ERR){
                serverLog(LL_WARNING, "Redis server wait for available space to sync write timeout");
                return C_ERR;
            }
            readn = MIN(ctx->rx_offset - ctx->recv_offset, size-nread);
            remain_time -=(mstime()-start);
        }
        if(remain_time <= 0){
            break;
        }
        memcpy(ptr, ctx->recv_buffer+ctx->recv_offset, readn);
        ptr +=readn;
        nread +=readn;
        ctx->recv_offset +=readn;       
        /*
         * if size is larger than receive buffer, case below may happen
         * we reset recv buffer pointer, and continue copy if not timeout
         */ 
        if(ctx->recv_offset == ctx->recv_length)
            syncRecvBuffer(ctx, rdma_conn->cm_id);
        remain_time -=(mstime()-start);
    }
    return nread;
}

static ssize_t connRdmaSyncReadLine(connection *c, char *ptr, ssize_t size, long long timeout){
   rdmaConnection *rdma_conn = (rdmaConnection *)(c);
    rdmaContext *ctx = (rdmaContext *)rdma_conn->cm_id->context;

    ssize_t nread = 0;
    bool line = false;
    long long remain_time = timeout, start = mstime();
    while( nread < size ){
        ssize_t readn = MIN(ctx->rx_offset - ctx->recv_offset, size-nread);
        while (readn == 0 && remain_time > 0){
            if(connRdmaWait(c, start, timeout) == C_ERR){
                serverLog(LL_WARNING, "Redis server wait for available space to sync write timeout");
                return C_ERR;
            }
            readn = MIN(ctx->rx_offset - ctx->recv_offset, size-nread);
            remain_time -=(mstime()-start);
        }
        if(remain_time <= 0){
            break;
        }
        for( int i = 0; i < readn; i++ ){
            char *c = ctx->recv_buffer + ctx->recv_offset + i;
            if( (*c) == '\n' ){
                if( i > 0 && *(c-1) == '\r' ){
                    readn = i;
                    line = true;
                    break;
                }else{
                    serverLog(LL_WARNING, "Redis server receive data format error");
                    return C_ERR;
                }
            }
        }
        readn +=(line ? 1 : 0);
        memcpy(ptr, ctx->recv_buffer+ctx->recv_offset, readn);
        ptr +=readn;
        nread +=readn;
        ctx->recv_offset +=readn;       
        /*
         * if size is larger than receive buffer, case below may happen
         * we reset recv buffer pointer, and continue copy if not timeout
         */ 
        if(ctx->recv_offset == ctx->recv_length)
            syncRecvBuffer(ctx, rdma_conn->cm_id);
        remain_time -=(mstime()-start);
        if( line )
            break;
    }
    return nread; 
}

static const char *connRdmaGetLastError(connection *conn){
    return strerror(conn->last_errno);
}

/*
 * Event handler for specific rdma connection type
 * called when completion channel has a happened event 
 * 1. load rdma recv buffer ranged [recv_offset, rx_offset]
 * into query_buf of client
 * 2. write response into rdma send buffer
 */
static int rdmaHandleRecv( rdmaContext *ctx,struct rdma_cm_id *cmid, rdmaCommand *cmd ){
    int ret = 0;

    uint64_t addr = ntohu64(cmd->addr);
    uint32_t rkey = ntohl(cmd->rkey);
    uint32_t length = ntohl(cmd->length);

    switch (cmd->opcode)
    {
    case REGISTER_LOCAL_ADDR:
        ctx->remote_buffer = addr;
        ctx->remote_key = rkey;
        ctx->tx_offset = 0;
        ctx->tx_length = length;
        if(rdmaAdjustSendBuffer(ctx, ctx->tx_length))
            ret = C_ERR;
        break;
    
    default:
        serverLog(LL_WARNING, "RDMA unknown command");
        ret = C_ERR;
        break;
    }
    return ret;
}
static void rdmaHandleSend( rdmaCommand *cmd ){
    cmd->used = 0;
}
static int rdmaHandleWrite( int byte_len ){
    serverLog(LL_DEBUG, "Redis server write %d amount of data via rdma", byte_len);
    return C_OK;
}
static int rdmaHandleRecvIMM(rdmaContext *ctx, struct rdma_cm_id *cmid, rdmaCommand *cmd, int offset){

    serverAssert(ctx->rx_offset + offset <= ctx->recv_length);
    ctx->rx_offset +=offset;
    return rdmaPostRecv(ctx, cmid, cmd);
}

//handle cq event
static int rdmaHandleCq(rdmaConnection *rdma_conn){
    int ret = C_OK;

    struct rdma_cm_id *cmid = rdma_conn->cm_id;
    rdmaContext *ctx = (rdmaContext *)cmid->context;
    struct ibv_comp_channel *comp_channel = ctx->comp_channel;
    struct ibv_cq *cq = ctx->cq;

    ret = ibv_get_cq_event(comp_channel, &cq, NULL );
    //above operation is block, so it's necessary to check EAGAIN errno.
    if( ret ){
        if( errno != EAGAIN ){
            ret = C_ERR;
            rdma_conn->conn.last_errno = errno;        
            serverLog(LL_WARNING, "get event notification from comp channel failed:%s", connGetLastError(errno) );
            return ret;
        }
    }
    if(ret = (ibv_req_notify_cq(cq, 0)) ){
        ret = C_ERR;
        rdma_conn->conn.last_errno = errno;
        serverLog(LL_WARNING, "rearmed notification for cq failed:%s", connGetLastError(errno));
        return ret;
    }

    struct ibv_wc wc[REDIS_MAX_SGE * 2];
    int cq_events = 0, tmp = 0;
    cq_events = ibv_poll_cq(cq, REDIS_MAX_SGE * 2, &wc);
    if( cq_events == 0 ){
        return C_OK;   
    }
    serverLog(LL_VERBOSE, "Redis server get total %d cq events from cq", cq_events);
    ibv_ack_cq_events(cq, cq_events);
    for( int i = 0; i < cq_events; i++ ){
        switch (wc[i].opcode)
        {
        case IBV_WC_RECV_RDMA_WITH_IMM:
            rdmaCommand *cmd = (rdmaCommand *)(wc[i].wr_id);
            if (rdmaHandleRecvIMM(ctx,cmid, cmd, wc[i].byte_len)){
                rdma_conn->conn.state = CONN_STATE_ERROR;
                return C_ERR;
            }
            serverLog(LL_VERBOSE, "Redis server receive %d amount of data from client", wc[i].byte_len);
            break;

        case IBV_WC_RECV:
            rdmaCommand *cmd = (rdmaCommand *)(wc[i].wr_id);
            if (rdmaHandleRecv(ctx, cmid, cmd)){
                rdma_conn->conn.state = CONN_STATE_ERROR;
                return C_ERR;
            }
            serverLog(LL_VERBOSE, "Redis server receive command with from client with recv buffer:%x", ntohu64(cmd->addr));
            break;

        case IBV_WC_SEND:
            rdmaCommand *cmd = (rdmaCommand *)(wc[i].wr_id);
            rdmaHandleSend(cmd);
            serverLog(LL_VERBOSE, "Redis server send command with recv buffer addr %x, length %d", ntohu64(cmd->addr), ntohl(cmd->length));
            break;
        case IBV_WC_RDMA_WRITE:
            rdmaHandleWrite(wc[i].byte_len);
            serverLog(LL_VERBOSE, "Redis server write %d amount of data to client", wc[i].byte_len);
            break;
        default:
            break;
        }
    }
    /*while ((ibv_poll_cq(cq, 1, &wc)) > 0)
    { 
        switch (wc.opcode)
        {
            // IBV_WR_WRITE_WITH_IMM notification
        case IBV_WC_RECV_RDMA_WITH_IMM:
            rdmaCommand *cmd = (rdmaCommand *)wc.wr_id;
            ctx->rx_offset = wc.byte_len;
            if (rdmaHandleRecv(conn, cmd)){
                ibv_ack_cq_events(cq, cq_events);
                return C_ERR;
            }
            break;
        // IBV_WR_SEND notification
        case IBV_WC_RECV:
            // receive command from remote node
            rdmaCommand *cmd = (rdmaCommand *)wc.wr_id;
            if (rdmaHandleRecv(conn, cmd)){
                ibv_ack_cq_events( cq, cq_events);
                return C_ERR;
            }
            break;
        }
    }*/

    return ret;
} 

static void connRdmaEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask ){

    rdmaConnection *rdma_conn = (rdmaConnection *)(clientData);
    connection *conn = &rdma_conn->conn;
    rdmaContext *ctx = (rdmaContext *)rdma_conn->cm_id->context;

    if(rdmaHandleCq( rdma_conn ) == C_ERR)
        return C_ERR;

    int call_read = (mask & AE_READABLE) && conn->read_handler;
    int call_write = (conn->write_handler != NULL);
    if( call_read ){
        while( ctx->recv_offset < ctx->rx_offset ){
            if(!callHandler(conn, conn->read_handler)) return;
        }
    }

    //it's time to reuse the receive buffer
    if( ctx->recv_offset == ctx->recv_length )
        //reset rx_offset and recv_offset
        syncRecvBuffer(ctx, rdma_conn->cm_id);


    //when there is something to write, just execute rdma write
    if( conn->write_handler && !callHandler(conn, conn->write_handler))
        return;
}

/*
 *  Redis server File event and Time event registeration
 *  1. call readQueryFromClient to read command from receive 
 *  buffer and fill up querybuf.
 *  2. call writeToClient to write responce to client from obuf
 *  3. periodically call rdmaEventHandler to handle event on 
 *  completion channel
 */ 

int connRdmaCron(struct aeEventLoop *eventLoop, long long id, void *clientData){

    connection *c = (connection *)(clientData);
    rdmaConnection *rdma_conn = (rdmaConnection *)(c);
    if( c->state != CONN_STATE_CONNECTED )
        return REDIS_SYNC_MS;
    //rdmaHandleCq(c);
    connRdmaEventHandler(eventLoop, c->fd, c, AE_READABLE);
    return REDIS_SYNC_MS;

}
//install connRdmaEventHandler call back into ae event manager
static int connRdmaSetIOHandler(connection *c){
    rdmaConnection *rdma_conn = (rdmaConnection *)(c);
    struct rdma_cm_id *cmid = rdma_conn->cm_id;
    rdmaContext *ctx = (rdmaContext *)(cmid->context);
    
    //install connection object into rdmaContext
    ctx->connection = c;

    if( c->read_handler || c->write_handler ){
        if(aeCreateFileEvent(server.el, c->fd, AE_READABLE, c->type->ae_handler, c) == AE_ERR)
        return C_ERR;
        if( ctx->timeevent == -1 ){
            ctx->timeevent = aeCreateTimeEvent(server.el, REDIS_SYNC_MS, connRdmaCron, c, NULL );
            if ( ctx->timeevent == ANET_ERR ){
                return C_ERR;
            }
        }
    }else{
        aeDeleteFileEvent(server.el, c->fd, AE_READABLE);
        if (ctx->timeevent != -1) {
            aeDeleteTimeEvent(server.el, ctx->timeevent);
            ctx->timeevent = -1;
        }
    }
    return C_OK;
}

static int connRdmaSetReadHandler(connection *c, ConnectionCallbackFunc func){
    //func == readQueryFromClient
    if( func == c->read_handler ) return C_OK;

    c->read_handler = func;

    return connRdmaSetIOHandler( c );
}

static int connRdmaSetWriteHandler(connection *c, ConnectionCallbackFunc func, int barrier){
    //func == writeToClient
    if( func == c->write_handler ) return C_OK;

    c->write_handler = func;
    if(barrier)
        c->flags |= CONN_FLAG_WRITE_BARRIER;
    else
        c->flags &= ~CONN_FLAG_WRITE_BARRIER;
    
    return connRdmaSetIOHandler( c ); 
}

static int connRdmaAccept(connection *c, ConnectionCallbackFunc accept_handler ){
    int ret = C_OK;

    if( c->state != CONN_STATE_ACCEPTING ) return C_ERR;
    c->state = CONN_STATE_CONNECTED;

    connIncrRefs(c);
    if(!callHandler(c, accept_handler)) ret = C_ERR;
    connDecrRefs(c);

    return ret;
}

/* 
 * Rdma connection between server and client
 * Maintain a data structure connection per server-client
 * 1. Handle connection handshake during establishment
 * create and allocate resources necessarily for rdma Communication
 * 2. Handle disconnection and close of the connection, 
 * deallocate memory and destroy necessary resources.
 */
connection *connCreateRdma(){
    rdmaConnection *rdma_conn = zcalloc(sizeof(rdmaConnection));
    rdma_conn->conn.type = CONN_TYPE_RDMA;
    rdma_conn->conn.fd = -1;
    return rdma_conn;
}
connection *connCreateAcceptedRdma(int fd, struct rdma_cm_id *cmid){
    rdmaConnection *rdma_conn = (rdmaConnection *)(connCreateRdma);
    rdma_conn->conn.fd = fd;
    rdma_conn->conn.state = CONN_STATE_ACCEPTING;
    rdma_conn->cm_id = cmid;
    return rdma_conn;
}

int createRdmaConnResources(rdmaContext *ctx,struct rdma_cm_id *cm_id ){
    struct ibv_pd *pd;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;

    //ctx注入进cm_id中
    cm_id->context = ctx;

    pd = ibv_alloc_pd(cm_id->verbs);
    if( !pd ){
        serverLog(LL_WARNING, "Redis server allocate protection domain failed");
        goto freeresources;
    }
    ctx->pd = pd;
    comp_channel = ibv_create_comp_channel(cm_id->verbs);
    if( !comp_channel ){
        serverLog(LL_WARNING, "Redis server create completion channel failed");
        goto freeresources; 
    }
    ctx->comp_channel = comp_channel;
    cq = ibv_create_cq(cm_id->verbs, REDIS_MAX_SGE * 2, NULL, comp_channel, 0);
    if( !cq ){
        serverLog(LL_WARNING, "Redis server create ompletion queue failed");
        goto freeresources;
    }
    ctx->cq = cq;
    // second parameter: solicited only(only notify if WR is flagged as solicited)
    if(ibv_req_notify_cq(cq,0)){
        serverLog(LL_WARNING,"Redis server arm notification for specific cq failed:%s", strerror(errno));
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
        serverLog(LL_WARNING, "create qp failed:%s", strerror(errno) );
        goto freeresources;
    }
    //set up memory region
    if(rdmaSetupIOBuffer(ctx, cm_id))
        goto freeqp;
    return C_OK;


freeqp:
    rdma_destroy_qp(cm_id->qp);
freeresources:
    if (ctx->cq)
        ibv_destroy_cq(ctx->cq);
    if (ctx->comp_channel)
        ibv_destroy_comp_channel(ctx->comp_channel);
    if (ctx->pd)
        ibv_dealloc_pd(ctx->pd);

    return C_ERR;
}

static void redisRdmaFree(rdmaContext *ctx){

    if( !ctx ) return;
    if(ibv_destroy_cq(ctx->cq))
        serverLog(LL_WARNING, "Redis client destroy completion queue failed:%s", strerror(errno));
    rdmaDestroyIOBuffer(ctx);
    if (ibv_destroy_comp_channel(ctx->comp_channel))
        serverLog(LL_WARNING, "Redis client destroy completion channel failed:%s", strerror(errno));
    ibv_dealloc_pd(ctx->pd);

}

static int rdmaHandleConnectRequest(struct rdma_cm_event *event, char *ip, size_t ip_len, int *port){
    struct rdma_cm_id *eid = event->id;
    struct sockaddr_storage sa;
    rdmaContext *ctx = zmalloc(sizeof(rdmaContext));
    ctx->timeevent = -1;
    memcpy(&sa, &eid->route.addr.dst_storage, sizeof(sa));

    if( sa.ss_family == AF_INET ){
        struct sockaddr_in *s = (struct sockaddr_in *)(&sa);
        if(ip) inet_ntop(AF_INET, (void *)(&s->sin_addr), ip, ip_len);
        if(port) *port = ntohs(s->sin_port);

    }else{
       struct sockaddr_in6 *s = (struct sockaddr_in6 *)(&sa);
        if(ip) inet_ntop(AF_INET6, (void *)(&s->sin6_addr), ip, ip_len);
        if(port) *port = ntohs(s->sin6_port);
    }

    //创建资源，rdma_create_qp, ibv_create_pd, ibv_create_cq_channel
    if(createRdmaConnResources(ctx, eid) == C_ERR){
        goto freectx;
    }

    struct rdma_conn_param cm_params = {
        .responder_resources = 1,
        .initiator_depth = 1,
        .retry_count = 5
    };
    if(rdma_accept(eid, &cm_params)){
        serverLog(LL_WARNING, "Redis server rdma_accept failed");
        goto releaseResource;
    }
    return C_OK;

releaseResource:
    redisRdmaFree( ctx );
freectx:
//rdma_reject
    rdma_destroy_id(eid);
    zfree(ctx);

    return C_ERR;
}

static int rdmaHandleEstablished( struct rdma_cm_event *event){
    struct rdma_cm_id *cmid = event->id;
    rdmaContext *ctx = (rdmaContext *)(cmid->context);
    if(syncRecvBuffer( ctx, cmid ))
        return C_ERR;
    return C_OK;
}
//handle DISCONNECTED
static int rdmaHandleDisconnection( struct rdma_cm_event *event ){
    struct rdma_cm_id *cmid = event->id;
    rdmaContext *ctx = (rdmaContext *)(cmid->context);
    connection *conn = ctx->connection;
    
    conn->state = CONN_STATE_CLOSED;
    if( conn->read_handler != NULL )
        callHandler(conn, conn->read_handler);
    if(conn->write_handler != NULL )
        callHandler(conn, conn->write_handler);

    return C_OK;
}
/* close the connection and free resources */
static void connRdmaClose( connection *conn ){
    rdmaConnection *rdma_conn = (rdmaConnection *)(conn);
    struct rdma_cm_id *cmid = rdma_conn->cm_id;

    //delete comp channel corresponding event in event manager
    if( conn->fd != -1 ){
        aeDeleteFileEvent(server.el, conn->fd, AE_READABLE);
        conn->fd = -1;
    }
    if( !cmid )
        goto free_conn;
    
    rdmaContext *ctx = (rdmaContext *)(cmid->context);
    if( ctx->timeevent != -1 ){
        aeDeleteTimeEvent(server.el, ctx->timeevent);
        ctx->timeevent = -1;
    }

    if(!rdma_disconnect( cmid ))
        rdmaHandleCq(rdma_conn);
    else
        serverLog(LL_WARNING, "rdma_disconnect failed:%s", strerror(errno));

    redisRdmaFree(ctx);

    if( cmid->qp )
        ibv_destroy_qp(cmid->qp);
    rdma_destroy_id(cmid);
    rdma_conn->cm_id = NULL;
    zfree(ctx);    

free_conn:
    zfree(rdma_conn); 

}

static int connRdmaGetType(connection *c){
    (void)c;

    return CONN_TYPE_RDMA;
}

ConnectionType CT_rdma = {
    .ae_handler = connRdmaEventHandler,
    .close = connRdmaClose,
    .write = connRdmaWrite,
    .read = connRdmaRead,
    .accept = connRdmaAccept,
    .connect = connRdmaConnect,
    .set_read_handler = connRdmaSetReadHandler,
    .set_write_handler = connRdmaSetWriteHandler,
    .get_last_error = connRdmaGetLastError,
    .blocking_connect = connRdmaBlockingConnect,
    .sync_write = connRdmaSyncWrite,
    .sync_read = connRdmaSyncRead,
    .sync_readline = connRdmaSyncReadLine,
    .get_type = connRdmaGetType
};

/* analygy to anetTcpAccept(...) */
#define MAX_ACCEPTS_PER_CALL 100
/*
 * @param s: listening channel对于的fd
 * @param private: 记录连接建立时新生成的cm id
 * @return: 新的连接建立后对应的completion channel对应的fd
 */
int netRdmaAccept(char *err, int s, char *ip, size_t ip_len,int *port, void **privdata ){
    int ret = C_OK;
    struct rdma_cm_event *event;
    if( rdma_get_cm_event(listen_channel, &event) == -1 ){
        return ANET_ERR;
    }

    switch (event->event)
    {
    case RDMA_CM_EVENT_CONNECT_REQUEST:
        ret = rdmaHandleConnectRequest(event,ip,ip_len, port);
        if( ret == C_OK ){
            *privdata = event->id;
            rdmaContext *ctx = (rdmaContext *)event->id->context;
            ret = ctx->comp_channel->fd;
        }else
            ret = ANET_ERR;
        break;
    case RDMA_CM_EVENT_ESTABLISHED:
        ret = rdmaHandleEstablished( event );
        if( ret == C_ERR )
            ret = ANET_ERR;
        break;
    case RDMA_CM_EVENT_CONNECT_ERROR:
    case RDMA_CM_EVENT_REJECTED:
    case RDMA_CM_EVENT_DISCONNECTED:
    case RDMA_CM_EVENT_DEVICE_REMOVAL:
        ret = rdmaHandleDisconnection( event );
        if (ret == C_OK)
            ret = ANET_OK;
        else
            ret = ANET_ERR;
        break;

    default:
        break;
    }
    if( rdma_ack_cm_event(event)== -1){
        return ANET_ERR;
    }
    
    return ret;
}