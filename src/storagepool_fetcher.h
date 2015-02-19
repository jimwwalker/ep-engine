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

// TYNSET: Prototype fetcher that belongs to the pool (to replace bgFetcher which is owned by an engine)

#pragma once
#include "tasks.h"
#include "itemkey.h"
#include "bgfetcher.h" // TYNSET: need VBucketBGFetchItem and vb_bgfetch_queue_t

class StoragePoolFetcher;
class StoragePool;


/**
 * A task for fetching items from disk.
 */
class StoragePoolFetcherTask : public GlobalTask {
public:
    StoragePoolFetcherTask(EventuallyPersistentEngine *e, StoragePoolFetcher& f,
                  const Priority &p, double sleeptime, bool shutdown)
        : GlobalTask(e, p, sleeptime, shutdown), fetcher(f) { }

    bool run();

    std::string getDescription() {
        return std::string("StoragePool item fetcher");
    }

private:
    size_t taskId;
    StoragePoolFetcher& fetcher;
};

class StoragePoolFetcher {
public:
    StoragePoolFetcher(StoragePool& sp);
    void start();
    void stop();
    bool run();
    size_t doFetch(EventuallyPersistentEngine* currentEngine, uint16_t vb, vb_bgfetch_queue_t& items);
    void addPendingVB(bucket_id_t id, uint16_t vb);

private:
    StoragePool& storagePool;
    StoragePoolFetcherTask task;
    Mutex pendingMutex;
    std::unordered_map<bucket_id_t, std::set<uint16_t> > pending;
};

