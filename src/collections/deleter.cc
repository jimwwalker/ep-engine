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

#include "collections/deleter.h"
#include "collections/collections_dockey.h"

#include "ep_engine.h"

namespace Collections {

DeleterVisitor::DeleterVisitor(DeleterTask& deleter) : deleter(deleter) {
}

bool DeleterVisitor::visit(uint16_t vbid, HashTable& ht) {
    HashTable::Position ht_start;
    this->ht = &ht;
    ht.pauseResumeVisit(*this, ht_start);

    // When a VB is done, we can call vb->completeCollectionDeletion...

    return true;
}

bool DeleterVisitor::visit(const HashTable::HashBucketLock& hbl,
                           StoredValue& v) {
    if (deleter.isCandidate(v)) {
        ht->unlocked_del(hbl, v.getKey());
    }
    return true;
}

DeleterTask::DeleterTask(KVBucket& e)
    : ::GlobalTask(&e.getEPEngine(), TaskId::CollectionsDeleter, INT_MAX, true),
      targets(),
      description("CollectionsDeleter"),
      visitor(std::make_unique<DeleterVisitor>(*this)),
      epstore_position(e.startPosition()) {
}

bool DeleterTask::run() {
    if (engine->getEpStats().isShutdown) {
        return false;
    }

    // Setup that we will sleep forever when done.
    snooze(INT_MAX);

    // Clear the notification flag
    notified.store(false);

    epstore_position = engine->getKVBucket()->pauseResumeVisit(
            *visitor.get(), epstore_position);
    if (engine->getEpStats().isShutdown) {
        return false;
    }
    return true;
}

void DeleterTask::wakeup() {
    ExecutorPool::get()->wake(getId());
}

bool DeleterTask::isCandidate(StoredValue& v) {
    std::lock_guard<std::mutex> lg(targetsLock);

    for (auto& e : targets) {
        auto cDoc = Collections::DocKey::make(v.getKey(), "::");
        if (e.collection.compare(0,
                                 e.collection.size(),
                                 (const char*)cDoc.data(),
                                 cDoc.getCollectionLen()) == 0) {
            std::cerr << "Found a candidate " << e.collection << " for key: "
                      << std::string((const char*)cDoc.data(), cDoc.size())
                      << std::endl;
        }
    }
    return false;
}

void DeleterTask::scheduleCollectionDeletion(cb::const_char_buffer collection,
                                             uint32_t revision,
                                             int64_t seqno) {
    {
        std::lock_guard<std::mutex> lg(targetsLock);
        targets.push_back({cb::to_string(collection), revision, seqno});
    }

    bool expected = false;
    if (notified.compare_exchange_strong(expected, true)) {
        wakeup();
    }
}

Deleter::Deleter(KVBucket& e) : task(make_STRCPtr<DeleterTask>(e)) {
    ExecutorPool::get()->schedule(task);
}

void Deleter::stop() {
    ExecutorPool::get()->cancel(task->getId());
}

void Deleter::scheduleCollectionDeletion(cb::const_char_buffer collection,
                                         uint32_t revision,
                                         int64_t seqno) {
    ((DeleterTask*)task.get())
            ->scheduleCollectionDeletion(collection, revision, seqno);
}

} // end namespace Collections
