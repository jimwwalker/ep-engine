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
#include <unordered_set>
#include <stdint.h>

#include "storagepool_shard.h"
#include "configuration.h"

class HashTable;
class EventuallyPersistentEngine;

class StoragePool {
public:
    StoragePool();
    ~StoragePool();

    /*
        Obtain the pools configuration setting
    */
    Configuration& getConfiguration();

    /*
        Create a new engine (bucket)
    */
    EventuallyPersistentEngine* createEngine(GET_SERVER_API get_server_api);

    HashTable& getOrCreateHashTable(uint16_t vbid);

    /*
        Obtain a reference to the StoragePoolShard who will flush/fetch
        the specified vb (vbid)
    */
    StoragePoolShard& getStoragePoolShard(uint16_t vbid);

    /*
        Wake shard 0's flusher for flush-all for the bucket.
    */
    void wakeFlusherForFlushAll(bucket_id_t bucketId);

    /*
        Return the engine handle of the bucket id (or nullptr if not found)
    */
    EventuallyPersistentEngine* getEngine(bucket_id_t id);

    /*
        Called when an engine is going away, ensures global flusher completes work
    */
    void engineShuttingDown(EventuallyPersistentEngine* engine);

    /*
        Resume flushing/persistence for the specified bucket
    */
    void resumeFlushing(bucket_id_t bucketId);

    /*
        Pause flushing/persistence for the specified bucket
    */
    void pauseFlushing(bucket_id_t bucketId);

    /*
        Is flushing/persistence paused for the specified bucket?
    */
    bool isFlushingPaused(bucket_id_t bucketId);

    static StoragePool& getStoragePool();
    static void shutdown();

private:

    /*
        FUTURE: many storage pools...
    */
    static StoragePool* thePool;

    void removeEngine(EventuallyPersistentEngine* engine);

    Configuration config;
    std::vector< std::unique_ptr<HashTable> > hashTables;
    /*
        Storage pool provides flushing and fetching.
        Chunks of VBuckets are flushed and fetched by a shard.
    */
    std::vector< std::unique_ptr<StoragePoolShard> > shards;

    Mutex engineMapLock;
    std::unordered_map<bucket_id_t, EventuallyPersistentEngine*> engineMap;
    std::unordered_set<bucket_id_t> bucketsPaused;

};
