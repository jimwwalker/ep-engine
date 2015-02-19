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

// TYNSET: Prototype fetcher that belongs to the pool (to replace bgFetcher which belongs to one engine)


#include "storagepool_fetcher.h"
#include "vbucket.h"
#include "executorpool.h"
#include "ep_engine.h"
#include "storagepool.h"

bool StoragePoolFetcherTask::run() {
    return fetcher.run();
}

StoragePoolFetcher::StoragePoolFetcher(StoragePool& sp) : storagePool(sp), task(NULL, *this, Priority::BgFetcherPriority, 0.0, false) { // TYNSET FIX ME (NULL)

}

void StoragePoolFetcher::start() {
    ExecutorPool* iom = ExecutorPool::get();
    iom->schedule(&task, READER_TASK_IDX);
}

void StoragePoolFetcher::stop() {
    ExecutorPool::get()->cancel(task.getId());
}

void StoragePoolFetcher::addPendingVB(bucket_id_t id, uint16_t vb) {
    std::cout << "addPending" << id << " " << vb << std::endl;
    LockHolder lockHolder(pendingMutex);
    pending[id].insert(vb);
}

size_t StoragePoolFetcher::doFetch(EventuallyPersistentEngine* currentEngine, uint16_t vb, vb_bgfetch_queue_t& items) {
    currentEngine->getEpStore()->getROUnderlying(vb)->getMulti(vb, items);
    std::vector<bgfetched_item_t> fetchedItems;
    size_t totalfetches = 0;
    for (auto fetchItr : items) {
        for (auto item : fetchItr.second) {
            const ItemKey &key = fetchItr.first;
            fetchedItems.push_back(std::make_pair(key, item));
            ++totalfetches;
        }


    }
    return totalfetches;
}

bool StoragePoolFetcher::run() {
    // fetch items, no throttle. try for *everything*
    LockHolder lockHolder(pendingMutex);
    size_t totalFetched = 0;
    for (auto bucketPendingItems : pending) {
        std::cout << "OK processing " << bucketPendingItems.first << std::endl;
        // first is bucket_id, second is list of vb
        for (auto pendingVb : bucketPendingItems.second) {
            EventuallyPersistentEngine* currentEngine = nullptr;
            if (storagePool.getEngine(bucketPendingItems.first, &currentEngine)) {
                RCPtr<VBucket> vb = currentEngine->getEpStore()->getVBucket(pendingVb);
                vb_bgfetch_queue_t itemsForFetching;
                if (vb && vb->getBGFetchItems(itemsForFetching)) {
                    totalFetched += doFetch(currentEngine, pendingVb, itemsForFetching);
                }
            }
            // else engine gone away...
            // TYNSET: delete bgFetchItems?
        }
    }
    return true;
}