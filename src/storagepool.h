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
#include <memory>
#include <vector>
#include <stdint.h>

#include "configuration.h"

class HashTable;
class HashTableStorage;
class EventuallyPersistentEngine;

class StoragePool {
public:

    StoragePool();
    ~StoragePool();

    /*
        Create a new HashTable for the engine's vbucket.
    */
    HashTable& createHashTable(EventuallyPersistentEngine& engine, uint16_t vbid);

     /*
        Parse config string into storage pools config
        TYNSET: Warning, the pool is taking the config of the first bucket created.
        This should move towards using a config string when the pool is created.
    */
    void configure(const char* cfg, SERVER_HANDLE_V1* sapi);

    /*
        Obtain the pools configuration setting
    */
    Configuration& getConfiguration() {
        return config;
    }

    static StoragePool& getStoragePool();

private:

    static StoragePool thePool;

    Configuration config;
    bool configured;
    std::vector< std::unique_ptr<HashTableStorage> > hashTableStorage;

};
