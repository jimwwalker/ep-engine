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

/*
 * StoragePool fetcher.
 *
 * A fetcher task that services the vbuckets of many buckets.
 */
#pragma once
#include "tasks.h"
#include "itemkey.h"
#include "kvstore.h"

class StoragePoolFetcher;
class StoragePool;
class StoragePoolShardTaskable;

/*
 * GlobalTask sub-class to schedule the main task code
 */
class StoragePoolFetcherTask : public GlobalTask {
public:
    StoragePoolFetcherTask(Taskable *t, StoragePoolFetcher& f,
                  const Priority &p, double sleeptime, bool shutdown)
        : GlobalTask(t, p, sleeptime, shutdown), fetcher(f) { }

    bool run();

    std::string getDescription() {
        return std::string("StoragePool item fetcher");
    }

private:
    StoragePoolFetcher& fetcher;
};

class StoragePoolFetcher {
public:

    StoragePoolFetcher(StoragePool& sp, StoragePoolShardTaskable& spt);

    /*
     * offtask
     * Start the storagepool fetcher task.
     */
    void start();

    /*
     * offtask
     * Stop the storagepool fetcher task.
     */
    void stop();

    /*
     * ontask
     * Run the storagepool fetcher task code.
     */
    bool run();

    /*
     * ontask
     * Perform fetches for the bucket's VB.
     */
    size_t doFetch(EventuallyPersistentEngine* currentEngine,
                   uint16_t vb,
                   vb_bgfetch_queue_t& items);

    /*
     * offtask
     * The bucket:vb requires a fetch (or fetches).
     */
    void addPendingVB(bucket_id_t id, uint16_t vb);

private:

    /*
     * offtask
     * request that the global task code schedules the fetcher task.
     */
    void wake();

    /*
     * ontask
     * Perform a fetch for all pending buckets.
     */
    void fetchAllBuckets();

    /*
     * The Id of the task (for global thread pool code).
     */
    size_t taskId;

    /*
     * The pool this fetcher belongs to.
     */
    StoragePool& storagePool;

    /*
     * The pool-shard this fetcher belongs to.
     */
    StoragePoolShardTaskable& taskable;

    /*
     * Mutex to serialise access to the pending map.
     */
    Mutex pendingMutex;

    /*
     * A map of every pending bucket to its pending vbuckets.
     */
    std::unordered_map<bucket_id_t, std::set<uint16_t> > pending;
};
