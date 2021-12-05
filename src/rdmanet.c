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

#define MIN(a, b)  \
    (a) < (b) ? a : b

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
    struct rdma_event_channel *comp_channel;
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
     */
    rdmaCommand *cmd_buf;
    struct ibv_mr *cmd_region;
};

/*
 * rdmaConnection class for each server-client rdma connection
 * analygy to connection class for server-client tcp connection
 */ 
struct rdmaConnection{
    connection *conn;
    struct rdma_cm_id *cm_id;
};

/**
 *  1. memory region addr and offset info synchronization 
 *  2. buffer read/write pointer synchronization 
 */
static int rdmaSendCommand( connection *conn,rdmaCommand *cmd ){
    int ret = 0;
    rdmaCommand *send_cmd;
    rdmaConnection *rdma_conn = (rdmaConnection *)(conn);
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    struct ibv_send_wr wr, *bad_wr;
    struct ibv_sge sgl;
    memset(&wr, 0, sizeof(wr));

    send_cmd->addr = htonu64(cmd->addr);
    send_cmd->length = htonl(cmd->length);
    send_cmd->rkey = htonl(cmd->rkey);

    send_cmd->opcode = REGISTER_LOCAL_ADDR;
    send_cmd->used = 1;
    send_cmd->version = 1;

    sgl.addr = send_cmd;
    sgl.length = sizeof(rdmaCommand);
    
    wr.num_sge = 1;
    wr.sg_list = &sgl;
    wr.send_flags |= IBV_SEND_INLINE;
    wr.send_flags |= IBV_SEND_SIGNALED;
    wr.opcode = IBV_WR_SEND;
    wr.next = NULL;
    wr.wr_id = (uint64_t)send_cmd;

    ret = ibv_post_send(cm_id->qp, &wr, &bad_wr);
    if( ret ){
       conn->last_errno = errno;
       serverLog(LL_WARNING, "RDMA: SEND command failed:%s", connGetLastError(errno));
       rdma_conn->conn->state = CONN_STATE_ERROR;
    }
    return ret;
}

static void syncRecvBuffer( connection *conn ){
    rdmaConnection *rdma_conn = (rdmaConnection *)conn;
    rdmaContext *ctx = (rdmaContext *)rdma_conn->cm_id->context;
    rdmaCommand cmd;
    
    cmd.addr = (uint64_t)ctx->recv_buffer;
    cmd.length = ctx->recv_length;
    cmd.rkey = ctx->recv_region->rkey;

    ctx->recv_offset = 0;
    ctx->rx_offset = 0;

    rdmaSendCommand(conn, &cmd);
}

/**
 * network I/O reimplemented in RDMA communication
 */

static int rdmaPostRecv( connection *conn, rdmaCommand *cmd ){
    int ret = 0;
    rdmaConnection *rdma_conn = (rdmaConnection *)(conn);
    rdmaContext *rdma_ctx = (rdmaContext *)rdma_conn->cm_id->context;

    struct ibv_recv_wr wr, *bad_wr;
    struct ibv_sge *sgl;
    memset(&wr, 0, sizeof(wr));

    sgl->addr = &cmd;
    sgl->length = sizeof(rdmaCommand);
    sgl->lkey = rdma_ctx->cmd_region->lkey;

    wr.num_sge = 1;
    wr.wr_id = (uint64_t)(cmd);
    wr.sg_list = &sgl;
    wr.next = NULL;

    if( ret = (ibv_post_recv(rdma_conn->cm_id->qp, &wr, &bad_wr)) ){
        conn->last_errno = errno;
        serverLog(LL_WARNING, "RDMA: post RECV failed:%s", connGetLastError(errno));    
        conn->state = CONN_STATE_ERROR;
    }
    return ret;
}

static int ibVerbsWrite(rdmaContext *ctx, size_t write_len){

    int ret = 0;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    uint32_t offset = ctx->tx_offset;
    rdmaConnection *rdma_conn = ctx->connection;
    struct rdma_cm_id *conn_id = rdma_conn->cm_id;
    memset(&wr, 0, sizeof(wr));

    sge.addr = ctx->send_buffer;
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

    ret = ibv_post_send( conn_id->qp, &wr, &bad_wr );
    if( ret ){
        rdma_conn->conn->last_errno = errno;
        serverLog(LL_WARNING, "RDMA: WRITE failed:%s", connGetLastError(errno));
        if( rdma_conn->conn->state == CONN_STATE_CONNECTED )
            rdma_conn->conn->state = CONN_STATE_ERROR;
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

static const char *connRdmaGetLastError(connection *conn){
    return strerror(conn->last_errno);
}

/*
 * event handler for specific rdma connection type
 * called when completion channel has a happened event 
 * 1. load rdma recv buffer ranged [recv_offset, rx_offset]
 * into query_buf of client
 * 2. write response into rdma send buffer
 */

//handle event when receive buffer has notification
static int rdmaHandleRecv( connection *conn, rdmaCommand *cmd ){
    rdmaConnection *rdma_conn = (rdmaConnection *)(conn);
    rdmaContext *ctx = (rdmaContext *)rdma_conn->cm_id->context;
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
        break;
    
    default:
        break;
    }

}

//handle cq event
static int rdmaHandeCq(connection *conn){
    int ret = 0;
    rdmaConnection *rdma_conn = (rdmaConnection *)(conn);
    rdmaContext *ctx = (rdmaContext *)rdma_conn->cm_id->context;
    struct rdma_event_channel *comp_channel = ctx->comp_channel;
    struct ibv_cq *cq = ctx->cq;

    ret = ibv_get_cq_event(comp_channel, &cq, NULL );
    if( ret ){
        if( errno != EAGAIN ){
            rdma_conn->conn->last_errno = errno;        
            serverLog(LL_WARNING, "get event notification from comp channel failed:%s", connGetLastError(errno) );
            return ret;
        }
    }
    ibv_req_notify_cq(cq, 0);

    struct ibv_wc wc;

    ibv_ack_cq_events(cq, 1);
    ibv_poll_cq(cq, 1, &wc);
    switch(wc.opcode){
        //IBV_WR_WRITE_WITH_IMM notification
        case IBV_WC_RECV_RDMA_WITH_IMM:
            rdmaCommand *cmd = (rdmaCommand *)wc.wr_id;
            ctx->rx_offset = wc.byte_len;
            if(rdmaHandleRecv( conn, cmd ))
                return C_ERR;
            break;
        case IBV_WC_RECV:
            //receive command from remote node
            rdmaCommand *cmd = (rdmaCommand *)wc.wr_id;
            if(rdmaHandleRecv( conn, cmd ))
                return C_ERR;
            break;
    }
    
    return ret;
} 
static void connRdmaEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask ){
    connection *conn = (connection *)(clientData);
    rdmaConnection *rdma_conn = (rdmaConnection *)(conn);
    rdmaContext *ctx = (rdmaContext *)rdma_conn->cm_id->context;

    rdmaHandleCq( rdma_conn );

    int call_read = (mask & AE_READABLE) && conn->read_handler;
    if( call_read ){
        while( ctx->recv_offset < ctx->rx_offset ){
            if(!callHandler(conn, conn->read_handler)) return;
        }
    }

    //it's time to reuse the receive buffer
    if( ctx->recv_offset == ctx->recv_length )
        //reset rx_offset and recv_offset
        syncRecvBuffer(conn);


    //when there is something to write, just execute rdma write
    if( conn->write_handler && !callHandler(conn, conn->write_handler))
        return;
}

static int connRdmaSetReadHandler(connection *c, ConnectionCallbackFunc func){

}
static int connRdmaSetWriteHandler(connection *c, ConnectionCallbackFunc func){

}



