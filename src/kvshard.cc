/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include "config.h"

#include <functional>

#include "ep_engine.h"
#include "kvshard.h"

KVShard::KVShard(uint16_t id, EventuallyPersistentStore &store) :
    shardId(id), highPrioritySnapshot(false),
    lowPrioritySnapshot(false),
    kvConfig(store.getEPEngine().getConfiguration(), shardId),
    highPriorityCount(0)
{
    Configuration &config = store.getEPEngine().getConfiguration();
    maxVbuckets = config.getMaxVbuckets();

    vbuckets = new RCPtr<VBucket>[maxVbuckets];

    std::string backend = kvConfig.getBackend();
    currCommitInterval = 1;

    if (backend.compare("couchdb") == 0) {
        rwUnderlying = KVStoreFactory::create(kvConfig,
                                              store.getEPEngine().getBucketId(),
                                              false);
        roUnderlying = KVStoreFactory::create(kvConfig,
                                              store.getEPEngine().getBucketId(),
                                              true);
    } else if (backend.compare("forestdb") == 0) {
        rwUnderlying = KVStoreFactory::create(kvConfig,
                                              store.getEPEngine().getBucketId());
        roUnderlying = rwUnderlying;
        currCommitInterval = config.getMaxVbuckets()/config.getMaxNumShards();
    }
    initCommitInterval = currCommitInterval;
}

KVShard::~KVShard() {
    delete rwUnderlying;

    /* Only couchstore has a read write store and a read only. ForestDB
     * only has a read write store. Hence delete the read only store only
     * in the case of couchstore.
     */
    if (kvConfig.getBackend().compare("couchdb") == 0) {
        delete roUnderlying;
    }

    delete[] vbuckets;
}

KVStore *KVShard::getRWUnderlying() {
    return rwUnderlying;
}

KVStore *KVShard::getROUnderlying() {
    return roUnderlying;
}

RCPtr<VBucket> KVShard::getBucket(uint16_t id) const {
    if (id < maxVbuckets) {
        return vbuckets[id];
    } else {
        return NULL;
    }
}

void KVShard::setBucket(const RCPtr<VBucket> &vb) {
    vbuckets[vb->getId()].reset(vb);
}

void KVShard::resetBucket(uint16_t id) {
    vbuckets[id].reset();
}

std::vector<int> KVShard::getVBucketsSortedByState() {
    std::vector<int> rv;
    for (int state = vbucket_state_active;
         state <= vbucket_state_dead;
         ++state) {
        for (size_t i = 0; i < maxVbuckets; ++i) {
            RCPtr<VBucket> b = vbuckets[i];
            if (b && b->getState() == state) {
                rv.push_back(b->getId());
            }
        }
    }
    return rv;
}

std::vector<int> KVShard::getVBuckets() {
    std::vector<int> rv;
    for (size_t i = 0; i < maxVbuckets; ++i) {
        RCPtr<VBucket> b = vbuckets[i];
        if (b) {
            rv.push_back(b->getId());
        }
    }
    return rv;
}

bool KVShard::setHighPriorityVbSnapshotFlag(bool highPriority) {
    bool inverse = !highPriority;
    return highPrioritySnapshot.compare_exchange_strong(inverse, highPriority);
}

bool KVShard::setLowPriorityVbSnapshotFlag(bool lowPriority) {
    bool inverse = !lowPriority;
    return lowPrioritySnapshot.compare_exchange_strong(inverse, lowPriority);
}

uint16_t KVShard::decrCommitInterval(void) {
    cb_assert(currCommitInterval != 0);
    --currCommitInterval;

    //When the current commit interval hits zero, then reset the
    //current commit interval to the initial value
    if (!currCommitInterval) {
        currCommitInterval = initCommitInterval;
        return 0;
    }

    return currCommitInterval;
}

void NotifyFlusherCB::callback(uint16_t &vb) {
    storagePoolShard->getFlusher()->addPendingVB(bucketId, vb);
}
