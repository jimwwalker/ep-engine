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

#include "executorpool.h"
#include "storagepool_shard.h"
#include "storagepool.h"

StoragePoolShard::StoragePoolShard(StoragePool& sp)
  : taskable(this, sp.getConfiguration()) {
    ExecutorPool::get()->registerTaskable(&taskable);
    fetcher = new StoragePoolFetcher(sp, taskable);
    flusher = new StoragePoolFlusher(sp, taskable);
    fetcher->start();
    flusher->start();
}

StoragePoolShard::~StoragePoolShard() {
    fetcher->stop();
    flusher->stop();
    // TYNSET TODO: Passing false is not correct?
    ExecutorPool::get()->unregisterTaskable(&taskable, false);
    delete fetcher;
    delete flusher;
}

StoragePoolFetcher* StoragePoolShard::getFetcher() {
    return fetcher;
}

StoragePoolFlusher* StoragePoolShard::getFlusher() {
    return flusher;
}

StoragePoolShardTaskable& StoragePoolShard::getTaskable() {
    return taskable;
}
