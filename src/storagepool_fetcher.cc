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
 * Background Fetcher Task which performs fetch on behalf of the buckets of a
 * single pool.
 */

#include "storagepool_fetcher.h"
#include "vbucket.h"
#include "executorpool.h"
#include "ep_engine.h"
#include "storagepool.h"

bool StoragePoolFetcherTask::run() {
    bool rv = fetcher.run();
    if (rv) {
        snooze(INT_MAX);
    }
    return rv;
}

StoragePoolFetcher::StoragePoolFetcher(StoragePool& sp,
                                       StoragePoolShardTaskable& spt)
    : storagePool(sp), taskable(spt) {
}

void StoragePoolFetcher::start() {
    ExTask task = new StoragePoolFetcherTask(&taskable,
                                             *this,
                                             Priority::BgFetcherPriority,
                                             0.0,
                                             false);
    taskId = task->getId();
    ExecutorPool::get()->schedule(task, READER_TASK_IDX);
}

void StoragePoolFetcher::stop() {
    ExecutorPool::get()->cancel(taskId);
}

void StoragePoolFetcher::wake() {
    ExecutorPool::get()->wake(taskId);
}

void StoragePoolFetcher::addPendingVB(bucket_id_t id, uint16_t vb) {
    LockHolder lockHolder(pendingMutex);
    pending[id].insert(vb);
    wake();
}

size_t StoragePoolFetcher::doFetch(EventuallyPersistentEngine* currentEngine,
                                   uint16_t vb,
                                   vb_bgfetch_queue_t& items) {
    hrtime_t startTime(gethrtime());
    currentEngine->getEpStore()->getROUnderlying(vb)->getMulti(vb, items);
    std::vector<bgfetched_item_t> fetchedItems;
    size_t totalfetches = 0;
    for (auto itemsItr : items) {
        for (auto item : itemsItr.second.bgfetched_list) {
            const ItemKey &key = itemsItr.first;
            fetchedItems.push_back(std::make_pair(key, item));
            ++totalfetches;
        }
    }

    if (totalfetches > 0) {
        currentEngine->getEpStore()->completeBGFetchMulti(vb,
                                                          fetchedItems,
                                                          startTime);
        currentEngine->getEpStats().
            getMultiHisto.add((gethrtime()-startTime)/1000, totalfetches);
    }

    // Now clean up
    for (auto itemsItr : items) {
        // every fetched item belonging to the same key shares
        // a single data buffer, just delete it from the first fetched item
        itemsItr.second.bgfetched_list.front()->delValue();
        for (auto item : itemsItr.second.bgfetched_list) {
            delete item;
        }
    }

    return totalfetches;
}

void StoragePoolFetcher::fetchAllBuckets() {
    LockHolder lockHolder(pendingMutex);
    size_t totalFetched = 0;

    auto pendingBucketIter = pending.begin();
    while (pendingBucketIter != pending.end()) {

        // first is bucket_id, second is list of vb
        auto iter = pendingBucketIter->second.begin();

        // iterate the set of VB ids.
        while (iter != pendingBucketIter->second.end()) {
            uint16_t vbid = *iter;
            // Remove the vbucket from the set before processing it.
            iter = pendingBucketIter->second.erase(iter);
            lockHolder.unlock();

            EventuallyPersistentEngine* currentEngine = nullptr;
            if ((currentEngine =
                 storagePool.getEngine(pendingBucketIter->first)) != nullptr) {
                RCPtr<VBucket> vb =
                    currentEngine->getEpStore()->getVBucket(vbid);
                vb_bgfetch_queue_t itemsForFetching;
                if (vb && vb->getBGFetchItems(itemsForFetching)) {
                    // set the engine for memory tracking
                    EventuallyPersistentEngine *epe =
                        ObjectRegistry::onSwitchThread(currentEngine, true);
                    totalFetched += doFetch(currentEngine,
                                            vbid,
                                            itemsForFetching);
                    ObjectRegistry::onSwitchThread(epe, false);
                }

            } else {
                // the engine has gone, empty the set and
                // move onto the next bucket
                lockHolder.lock();
                pendingBucketIter->second.clear();
                // leave while (iter != pendingBucketIter->second.end())
                break;
            }
            lockHolder.lock(); // lock for going round the loop again
        }

        // Note: we have exited the loop with lockHolder locked.

        // If the set is now empty, drop the bucket from the pending map
        if (pendingBucketIter->second.size() == 0) {
            pendingBucketIter = pending.erase(pendingBucketIter);
        }
        // else it's not empty, means a bucket/VB became again,
        // don't delete and try this bucket again

        // Note: lockHolder is currently locked to go round the loop again.
    }
}

bool StoragePoolFetcher::run() {
    // lock and see if the pending map is now empty.
    // Buckets may of become pending again...
    LockHolder lockHolder(pendingMutex);

    while (pending.size() > 0) {
        lockHolder.unlock();
        fetchAllBuckets();
        lockHolder.lock();
    }
    return true;
}