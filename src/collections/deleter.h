/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "globaltask.h"
#include "kv_bucket_iface.h"

#include <memory>
#include <vector>

namespace Collections {

class Deleter;
class DeleterTask;

class DeleterVisitor : public PauseResumeEPStoreVisitor,
                       public PauseResumeHashTableVisitor {
public:
    DeleterVisitor(DeleterTask& deleter);

    // Implementation of PauseResumeEPStoreVisitor interface:
    bool visit(uint16_t vbid, HashTable& ht) override;

    // Implementation of PauseResumeHashTableVisitor interface:
    bool visit(const HashTable::HashBucketLock& hbl, StoredValue& v) override;

private:
    VBucketPtr vb;
    DeleterTask& deleter;
    HashTable* ht;
};

class DeleterTask : public ::GlobalTask {
public:
    DeleterTask(KVBucket& e);

    bool run();

    void wakeup();

    cb::const_char_buffer getDescription() {
        return description;
    }

    bool isCandidate(StoredValue& v);

    void scheduleCollectionDeletion(cb::const_char_buffer collection,
                                    uint32_t revision,
                                    int64_t seqno);

private:
    // some list of collections i'm deleting? name and endseqno
    struct Target {
        std::string collection;
        uint32_t revision;
        int64_t endSeqno;
    };

    std::mutex targetsLock;
    std::vector<Target> targets;

    std::string description;

    std::unique_ptr<DeleterVisitor> visitor;

    // Opaque marker indicating how far through the epStore we have visited.
    KVBucketIface::Position epstore_position;

    std::atomic<bool> notified;
};

class Deleter {
public:
    Deleter(KVBucket& e);

    void stop();

    void scheduleCollectionDeletion(cb::const_char_buffer collection,
                                    uint32_t revision,
                                    int64_t seqno);

private:
    ExTask task;
};

} // end namespace Collections
