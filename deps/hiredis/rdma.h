/**
 * @file rdma.h
 * @author maoxiangyu
 * @brief 
 * @version 0.1
 * @date 2022-01-05
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef __HIREDIS_RDMA_H
#define __HIREDIS_RDMA_H

#include "hiredis.h"

int redisContextConnectRdma(redisContext *c, const char *addr, int port, const struct timeval *timeout);

#endif
