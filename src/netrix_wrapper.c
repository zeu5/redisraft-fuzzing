/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"
#include <libnetrixclient/netrix.h>

static int NetrixInit(RedisRaftCtx* rr, RedisRaftConfig* rc) {
    NetrixWrapper* wrapper = malloc(sizeof(NetrixWrapper));
    client_config config;
    wrapper->client = create_client(config);
    return 0;
}

