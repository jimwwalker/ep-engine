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

#include "storagepool.h"
#include "stored-value.h"


EventuallyPersistentStoragePool::EventuallyPersistentStoragePool() : hashTables(1024) /* TYNSET: some config value. */ {

}

EventuallyPersistentStoragePool::~EventuallyPersistentStoragePool() {
    hashTables.clear();
}

/**
    Return a HashTable reference for the given vbucket ID (vbid).
    The storage pool creates each HashTable on the first request, then
    returns the HashTable for all subsequent callers.
**/
HashTable& EventuallyPersistentStoragePool::getOrCreateHashTable(uint16_t vbid) {
    if(hashTables[vbid].get() == nullptr) {
        hashTables[vbid] = std::unique_ptr<HashTable>(new HashTable());
    }
    return (*hashTables[vbid].get());
}

EventuallyPersistentStoragePool EventuallyPersistentStoragePool::thePool;

/**
    Basic factory that returns one storage pool.

    Future: support many storage pools.
**/
EventuallyPersistentStoragePool& EventuallyPersistentStoragePool::getStoragePool() {
    return thePool;
}