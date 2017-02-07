/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "kv_bucket.h"

/**
 * Ephemeral Bucket
 *
 * A bucket type without any persistent data storage. Similar to memcache (default)
 * buckets, except with VBucket goodness - replication, rebalance, failover.
 */

class EphemeralBucket : public KVBucket {
public:
    EphemeralBucket(EventuallyPersistentEngine& theEngine);

    /// Eviction not supported for Ephemeral buckets - without some backing
    /// storage, there is nowhere to evict /to/.
    protocol_binary_response_status evictKey(const DocKey& key,
                                             uint16_t vbucket,
                                             const char** msg,
                                             size_t* msg_size) override {
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    /// File stats not supported for Ephemeral buckets.
    ENGINE_ERROR_CODE getFileStats(const void* cookie,
                                   ADD_STAT add_stat) override {
        return ENGINE_KEY_ENOENT;
    }

    /// Disk stats not supported for Ephemeral buckets.
    ENGINE_ERROR_CODE getPerVBucketDiskStats(const void* cookie,
                                             ADD_STAT add_stat) override {
        return ENGINE_KEY_ENOENT;
    }

    /**
     * Creates an EphemeralVBucket
     */
    RCPtr<VBucket> makeVBucket(VBucket::id_type id,
                               vbucket_state_t state,
                               KVShard* shard,
                               std::unique_ptr<FailoverTable> table,
                               NewSeqnoCallback newSeqnoCb,
                               vbucket_state_t initState,
                               int64_t lastSeqno,
                               uint64_t lastSnapStart,
                               uint64_t lastSnapEnd,
                               uint64_t purgeSeqno,
                               uint64_t maxCas,
                               const std::string& collectionsManifest) override;

    /// Do nothing - no flusher to notify
    void notifyFlusher(const uint16_t vbid) override {
    }

};
