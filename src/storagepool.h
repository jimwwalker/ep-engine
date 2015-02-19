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

#pragma once
#include <memory>
#include <vector>
#include <stdint.h>
#include <storagepool_shard.h>

class HashTable;
class EventuallyPersistentEngine;

class StoragePool {
public:
    StoragePool();
    ~StoragePool();

    EventuallyPersistentEngine* createEngine(GET_SERVER_API get_server_api);

    HashTable& getOrCreateHashTable(uint16_t vbid);

    /*
        Obtain a reference to the StoragePoolShard who will flush/fetch
        the specified vb (vbid)
    */
    StoragePoolShard& getStoragePoolShard(uint16_t vbid);

    /*
        TYNSET: This method only really exists because async readers/writers etc...
        don't have a reference to the engine they are working for. We can remove this
        by passing engine& deeper into bgfetcher/flushers etc...

        return true if the engine exists and set engine to the reference
    */
    bool getEngine(bucket_id_t id, EventuallyPersistentEngine** engine);

    static StoragePool& getStoragePool();

private:
    std::vector< std::unique_ptr<HashTable> > hashTables;
    /*
        Storage pool provides flushing and fetching.
        Chunks of VBuckets are flushed and fetched by a shard.
    */
    std::vector< std::unique_ptr<StoragePoolShard> > shards;

    Mutex engineMapLock;
    std::unordered_map<bucket_id_t, EventuallyPersistentEngine*> engineMap;

    /*
        FUTURE: many storage pools...
    */
    static StoragePool thePool;
};
