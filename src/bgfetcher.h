/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#ifndef SRC_BGFETCHER_H_
#define SRC_BGFETCHER_H_ 1

#include "config.h"

#include <list>
#include <set>
#include <string>

#include "common.h"
#include "item.h"
#include "stats.h"

class VBucketBGFetchItem {
public:
    VBucketBGFetchItem(const void *c, bool meta_only) :
         initTime(gethrtime()), metaDataOnly(meta_only), cookie(c)
    { }
    ~VBucketBGFetchItem() {}

    void delValue() {
        delete value.getValue();
        value.setValue(NULL);
    }

    GetValue value;

    hrtime_t initTime;
    bool metaDataOnly;
    const void * cookie;
    // TYNSET: To be improved. the couch-kvstore needs to create Items when fetching the key
    // it needs the bucket_id, so we stash it in this object. I'd prefer something else to this.
    bucket_id_t bucketId;
};
typedef unordered_map<ItemKey, std::list<VBucketBGFetchItem *>, ItemKeyHash> vb_bgfetch_queue_t;
typedef std::pair<ItemKey, VBucketBGFetchItem *> bgfetched_item_t;

// Forward declarations.
class EventuallyPersistentStore;
class KVShard;
class GlobalTask;

/**
 * Dispatcher job responsible for batching data reads and push to
 * underlying storage
 */
class BgFetcher {
public:
    static const double sleepInterval;
    /**
     * Construct a BgFetcher task.
     *
     * @param s the store
     * @param d the dispatcher
     */
    BgFetcher(EventuallyPersistentStore *s, KVShard *k, EPStats &st) :
        store(s), shard(k), taskId(0), stats(st), pendingFetch(false) {}
    ~BgFetcher() {
        LockHolder lh(queueMutex);
        if (!pendingVbs.empty()) {
            LOG(EXTENSION_LOG_DEBUG,
                    "Warning: terminating database reader without completing "
                    "background fetches for %ld vbuckets.\n", pendingVbs.size());
            pendingVbs.clear();
        }
    }

    void start(void);
    void stop(void);
    bool run(GlobalTask *task);
    bool pendingJob(void);
    void notifyBGEvent(void);
    void setTaskId(size_t newId) { taskId = newId; }
    void addPendingVB(uint16_t vbId) {
        LockHolder lh(queueMutex);
        pendingVbs.insert(vbId);
    }

 //   void addPendingVB(const EventuallyPersistentEngine& engine, uint16_t vbId) {
   //     LockHolder lh(queueMutex);
   ////     if (pending.count(engine) == 0) {
     //       pending[engine] = std::set<uint16_t>();
    //    }
    //    pending[engine].insert(vbId);
    //}

private:
    size_t doFetch(uint16_t vbId);
    void clearItems(uint16_t vbId);

    EventuallyPersistentStore *store;
    KVShard *shard;
    vb_bgfetch_queue_t items2fetch;
    size_t taskId;
    Mutex queueMutex;
    EPStats &stats;

    AtomicValue<bool> pendingFetch;
    std::set<uint16_t> pendingVbs;

    // pending collects the engines (buckets) and their vbuckets awaiting fetches.
    //std::unordered_map<std::reference_wrapper<const EventuallyPersistentEngine>, std::set<uint16_t> > pending;
};

#endif  // SRC_BGFETCHER_H_
