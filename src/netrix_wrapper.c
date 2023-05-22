/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"
#include <libnetrixclient/netrix.h>

static int NetrixInit(RedisRaftCtx* rr, RedisRaftConfig* rc) {
    NetrixWrapper* wrapper = malloc(sizeof(NetrixWrapper));
    netrix_client_config config;
    
    config.id = malloc(sizeof(char)*3);
    sprintf(config.id, "%d", (int )rc->id);
    config.info = NULL;
    config.listen_addr = malloc(sizeof(char)*300);
    config.netrix_addr = malloc(sizeof(char)*300);
    sprintf(config.listen_addr, "%s:%u", rc->netrix_listener_addr.host, rc->netrix_listener_addr.port);
    sprintf(config.netrix_addr, "%s:%u", rc->netrix_server_addr.host, rc->netrix_server_addr.port);
    
    wrapper->client = netrix_create_client(config);
    rr->netrix_wrapper = wrapper;
    
    return 0;
}

