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
#include "ep_engine.h"

StoragePool::StoragePool()
  : configured(false),
    hashTableStorage(1) {
    HashTableStorage::setDefaultNumBuckets(config.getHtSize());
    HashTableStorage::setDefaultNumLocks(config.getHtLocks());
}

StoragePool::~StoragePool() {
    hashTableStorage.clear();
}

/*
    Return a reference to a new HashTable for the given vbucket ID (vbid).
    The HashTable will be configured to use the correct underlying mutex and hash-bucket store.
*/
HashTable& StoragePool::createHashTable(EventuallyPersistentEngine& engine, uint16_t vbid) {
    if (hashTableStorage[vbid].get() == nullptr) {
        hashTableStorage[vbid] = std::unique_ptr<HashTableStorage>(new HashTableStorage());
    }

    return *(new HashTable(engine.getBucketId(), hashTableStorage[vbid].get(), engine.getEpStats()));
}

void StoragePool::configure(const char* cfg, SERVER_HANDLE_V1* sapi) {
    if (!configured) {
        config.parseConfiguration(cfg, sapi);
        hashTableStorage.resize(config.getMaxVbuckets());
        HashTableStorage::setDefaultNumBuckets(config.getHtSize());
        HashTableStorage::setDefaultNumLocks(config.getHtLocks());
        configured = true;
    }
}

Configuration& StoragePool::getConfiguration() {
    return config;
}


StoragePool StoragePool::thePool;

/**
    Basic factory that returns one storage pool.

    Future: support many storage pools.
**/
StoragePool& StoragePool::getStoragePool() {
    return thePool;
}
