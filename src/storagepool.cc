/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "storagepool.h"
#include "stored-value.h"
#include "storagepool_shard.h"
#include "ep_engine.h"


StoragePool::StoragePool() : hashTables(1024), shards(4) /* TYNSET: use config values. */ {

}

StoragePool::~StoragePool() {
    hashTables.clear();
    shards.clear();
}

/**
    Create an engine
**/
EventuallyPersistentEngine* StoragePool::createEngine(GET_SERVER_API get_server_api) {
    EventuallyPersistentEngine* engine = new EventuallyPersistentEngine(get_server_api);
    LockHolder lockHolder(engineMapLock);
    engineMap[engine->getBucketId()] = engine;
    return engine;
}

/**
    Return a HashTable reference for the given vbucket ID (vbid).
    The storage pool creates each HashTable on the first request, then
    returns the HashTable for all subsequent callers.
**/
HashTable& StoragePool::getOrCreateHashTable(uint16_t vbid) {
    if(hashTables[vbid].get() == nullptr) {
        hashTables[vbid] = std::unique_ptr<HashTable>(new HashTable());
    }
    return (*hashTables[vbid].get());
}

StoragePoolShard& StoragePool::getStoragePoolShard(uint16_t vbid) {
    if (shards[vbid % 4].get() == nullptr) {
        shards[vbid % 4] = std::unique_ptr<StoragePoolShard>(new StoragePoolShard(*this));
    }
    return (*shards[vbid % 4].get());
}

bool StoragePool::getEngine(bucket_id_t id, EventuallyPersistentEngine** engine) {
    LockHolder lockHolder(engineMapLock);
    bool rv = false;
    if (engineMap.count(id) > 0) {
        rv = true;
        *engine = engineMap[id];
    }
    return rv;
}

StoragePool StoragePool::thePool;

/**
    Basic factory that returns one storage pool.

    Future: support many storage pools.
**/
StoragePool& StoragePool::getStoragePool() {
    return thePool;
}