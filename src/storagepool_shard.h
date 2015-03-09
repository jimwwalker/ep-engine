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

#include "taskable.h"
#include "storagepool_fetcher.h"
#include "storagepool_flusher.h"

class StoragePool;
class StoragePoolShard;

class StoragePoolShardTaskable : public Taskable {
public:

    StoragePoolShardTaskable(StoragePoolShard* s) : myShard(s), prio(LOW_BUCKET_PRIORITY), needConfig(4, 4) {

    }

    std::string getName() const {
        return "StoragePoolShard";
    }

    uintptr_t getGID() const {
        return reinterpret_cast<uintptr_t>(myShard);
    }

    bucket_priority_t getWorkloadPriority() const {
        return prio; // TYNSET: prio should orginally come from config?
    }

    void setWorkloadPriority(bucket_priority_t prio) {
        this->prio = prio;
    }

    WorkLoadPolicy& getWorkLoadPolicy(void) {
        return needConfig;
    }

    void logQTime(type_id_t taskType, hrtime_t enqTime) {
        // no logging
        (void)taskType;
        (void)enqTime;
        return;
    }

    void logRunTime(type_id_t taskType, hrtime_t runTime) {
        // no logging
        (void)taskType;
        (void)runTime;
        return;
    }

private:
    StoragePoolShard* myShard;
    bucket_priority_t prio;
    WorkLoadPolicy needConfig; // TYNSET: Ideally from a config option.
};


class StoragePoolShard {
public:
    StoragePoolShard(StoragePool& sp);
    ~StoragePoolShard();

    StoragePoolFetcher* getFetcher();
    StoragePoolFlusher* getFlusher();
    StoragePoolShardTaskable& getTaskable();

private:
    StoragePoolFetcher* fetcher;
    StoragePoolFlusher* flusher;
    StoragePoolShardTaskable taskable;
};
