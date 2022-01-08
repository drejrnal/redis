/**
 * @file rdmaproto.c
 * @author maoxiangyu (you@domain.com)
 * @brief 
 * @version 0.1
 * @date 2021-12-09
 * 
 * @copyright Copyright (c) 2021
 * 
 */

#include "server.h"
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <netdb.h>

void rdmaNetSetError(char *err, const char *fmt, ...)
{
    va_list ap;

    if (!err) return;
    va_start(ap, fmt);
    vsnprintf(err, ANET_ERR_LEN, fmt, ap);
    va_end(ap);
}

#ifdef USE_RDMA
#ifdef __linux__
//listen_channel对应的file descriptor交由ae事件管理器管理
struct rdma_event_channel *listen_channel;

int rdmaServer( char *err, int port, char *bindaddr, int family, int backlog, struct rdma_cm_id **cmids ){
    /*
     * for debug purpose
     */
    
    struct sockaddr_storage ss;
    memset(&ss, 0, sizeof(ss));
    char addrpresent[64];

    int rv, ret = ANET_OK;
    int afonly = 1;
    struct addrinfo hints, *servinfo, *p;
    char _port[6];
    struct rdma_cm_id *listen_cmid;

    snprintf(_port, 6, "%d", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = family;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if (bindaddr && !strcmp("*", bindaddr))
        bindaddr = NULL;
    if (family == AF_INET6 && bindaddr && !strcmp("::*", bindaddr))
        bindaddr = NULL;

    if ((rv = getaddrinfo(bindaddr,_port,&hints,&servinfo)) != 0) {
        rdmaNetSetError(err, "%s", gai_strerror(rv));
        return ANET_ERR;
    } else if( !servinfo ){
        rdmaNetSetError(err, "%s", "Redis server get addr info failed");
        ret = ANET_ERR; 
        goto end;
    }
    if(rdma_create_id(listen_channel, &listen_cmid, NULL, RDMA_PS_TCP )){
        return ANET_ERR;
    }
    rdma_set_option(listen_cmid, RDMA_OPTION_ID, RDMA_OPTION_ID_AFONLY, &afonly, sizeof(afonly));

    for( p = servinfo; p != NULL; p = p->ai_next ){
        memcpy(&ss, p->ai_addr, sizeof(ss));
        if(ss.ss_family == AF_INET ){
            struct sockaddr_in *sin = (struct sockaddr_in *)(p->ai_addr);
            inet_ntop(AF_INET, &sin->sin_addr, addrpresent, sizeof(addrpresent));
            serverLog(LL_DEBUG, "listening cm_id bind to addr %s:%d", addrpresent, ntohs(sin->sin_port));
        }
        else if(ss.ss_family == AF_INET6){
            struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)(p->ai_addr);
            inet_ntop(AF_INET, &sin6->sin6_addr, addrpresent, sizeof(addrpresent));
            serverLog(LL_DEBUG, "listening cm_id bind to addr %s:%d", addrpresent, ntohs(sin6->sin6_port));
        }
        if(rdma_bind_addr(listen_cmid, p->ai_addr )){
            serverLog(LL_WARNING, "Rdma Server bind addr failed");
            goto err;
        }
        if(rdma_listen(listen_cmid, backlog)){
            serverLog(LL_WARNING, "Rdma Server create listening id failed");
            goto err;
        }
        serverLog(LL_DEBUG, "Rdma server listening on port %d, listening qp number %d", ntohs(rdma_get_src_port(listen_cmid)), listen_cmid->port_num);
        *cmids = listen_cmid;
        goto end;
    }
    if(p==NULL){
        goto err;
    }
err:
    serverLog(LL_WARNING, "Redis server initiate failed");
    if( listen_cmid )
        rdma_destroy_id(listen_cmid);
    ret = ANET_ERR;
    *cmids = NULL;
end:
    freeaddrinfo(servinfo);
    return ret;
}

void closeRdmaListeners( struct rdma_cm_id **cmids, int count ){
    for( int i = count; i >= 0; i-- ){
        if( cmids[i] != NULL ){
            rdma_destroy_id(cmids[i]);
            aeDeleteFileEvent(server.el, listen_channel->fd, AE_READABLE);
        }
    }
}
/*
 * Initialize a set of file descriptors to listen to specific port
 * 
 * listening file descriptors in RDMA Context are stored in the array rfd.fd
 * and their number is set in rfd.count
 */ 

int rdmaListenToPort( int port, socketFds *rfd, struct rdma_cm_id **listening_cmids ){
    int ret = C_OK, index = 0;
    char **bindaddr = server.bindaddr;
    int bindaddr_count = server.bindaddr_count;
    char *default_bindaddr[2] = {"*", "-::*"};

    /* Force binding of 0.0.0.0 if no bind address is specified. */
    if (server.bindaddr_count == 0) {
        bindaddr_count = 2;
        bindaddr = default_bindaddr;
    }
    listen_channel = rdma_create_event_channel();
    if(listen_channel == NULL ){
        serverLog(LL_WARNING, "Failed creating listening event channel %s",
                     strerror(errno));
        return C_ERR;
    }

    for( int j = 0; j < bindaddr_count; j++ ){
        char *addr = bindaddr[j];
        int optional = *addr == '-';
        if(optional) addr++;
        if(strchr(addr, ':')){
            ret = rdmaServer(server.neterr, port, addr, AF_INET6, server.tcp_backlog, &(listening_cmids[index]) );
        }else{
            ret = rdmaServer(server.neterr, port, addr, AF_INET, server.tcp_backlog, &(listening_cmids[index]) );
        }
        if( ret == ANET_ERR ){
            int net_errno = errno;
            serverLog(LL_WARNING,
                "Warning: Could not create server RDMA listening socket %s:%d: %s",
                addr, port, server.neterr);
            if(net_errno == EADDRNOTAVAIL && optional)
                continue;
            if(net_errno == ENOPROTOOPT || net_errno == EPROTONOSUPPORT || 
                net_errno == ESOCKTNOSUPPORT || net_errno == EPFNOSUPPORT || 
                net_errno == EAFNOSUPPORT )
                continue;
            closeRdmaListeners(listening_cmids, index);
            return C_ERR;
        }
        index++;
    }
    rfd->fd[rfd->count] = listen_channel->fd;
    anetNonBlock(NULL, listen_channel->fd);
    anetCloexec(listen_channel->fd);
    rfd->count++;
    return C_OK;
}
#else
"Build error, RDMA related library should be run on linux platform"
#endif
#else
int rdmaListenToPort( int port, socketFds *rfd, struct rdma_cm_id **listening_cmids ){
    fprintf(stderr, "Build with RDMA failed, need option declaration BUILD_RDMA=yes");
    return C_ERR;
}
#endif