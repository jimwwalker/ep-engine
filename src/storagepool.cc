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
#include "storagepool_shard.h"
#include "ep_engine.h"
#include "defragmenter.h"

StoragePool::StoragePool()
  : configured(false),
    hashTableStorage(1),
    shards(1),
    defragmenterTask(nullptr),
    taskable(nullptr),
    tasksCreated(false) {
    HashTableStorage::setDefaultNumBuckets(config.getHtSize());
    HashTableStorage::setDefaultNumLocks(config.getHtLocks());
}

StoragePool::~StoragePool() {
    // TYNSET TODO: passing false as last parameter is not correct.
    if (tasksCreated) {
        ExecutorPool::get()->stopTaskGroup(taskable->getGID(), NONIO_TASK_IDX, false);
    }
    hashTableStorage.clear();
    shards.clear();
}

/*
    Return a reference to a new HashTable for the given vbucket ID (vbid).
    The HashTable will be configured to use the correct underlying mutex and hash-bucket store.
*/
HashTable& StoragePool::createHashTable(bucket_id_t bucketId, uint16_t vbid, EPStats &stats) {
    if (hashTableStorage[vbid].get() == nullptr) {
        hashTableStorage[vbid] = std::unique_ptr<HashTableStorage>(new HashTableStorage());
    }

    return *(new HashTable(bucketId, hashTableStorage[vbid].get(), stats));
}

/*
    Create an engine
*/
EventuallyPersistentEngine* StoragePool::createEngine(GET_SERVER_API get_server_api) {
    EventuallyPersistentEngine* engine = new EventuallyPersistentEngine(get_server_api);
    LockHolder lockHolder(engineMapLock);
    engineMap[engine->getBucketId()] = engine;
    return engine;
}

EventuallyPersistentEngine* StoragePool::getEngine(bucket_id_t id) {
    LockHolder lockHolder(engineMapLock);
    EventuallyPersistentEngine* rv = nullptr;
    if (engineMap.count(id) > 0) {
        rv = engineMap[id];
    }
    return rv;
}

void StoragePool::engineShuttingDown(EventuallyPersistentEngine* engine) {
    // run the flusher to purge the engine
    for (auto &shard : shards) {
        if (shard.get() != nullptr) {
            // execution will block on the flusher
            shard.get()->getFlusher()->flushEngineAndWait(engine);
        }
    }
    removeEngine(engine);
}

StoragePoolShard* StoragePool::getStoragePoolShard(uint16_t vbid) {
    if (shards[vbid % config.getMaxNumShards()].get() == nullptr) {
        shards[vbid % config.getMaxNumShards()] = std::unique_ptr<StoragePoolShard>(new StoragePoolShard(*this));
    }
    return shards[vbid % config.getMaxNumShards()].get();
}

void StoragePool::configure(const char* cfg, SERVER_HANDLE_V1* sapi) {
    if (!configured) {
        config.parseConfiguration(cfg, sapi);
        hashTableStorage.resize(config.getMaxVbuckets());
        shards.resize(config.getMaxNumShards());
        HashTableStorage::setDefaultNumBuckets(config.getHtSize());
        HashTableStorage::setDefaultNumLocks(config.getHtLocks());
        taskable = new StoragePoolTaskable(this, config);
        configured = true;
    }
    if (!tasksCreated) {
        // bring tasks up when the first engine is initialized otherwise we will initialise various
        // globals from the default configuration.
        createTasks();
        tasksCreated = true;
    }
}

void StoragePool::wakeFlusherForFlushAll(bucket_id_t bucketId) {
    // explictly wake shard 0 for the bucketId
    shards[0]->getFlusher()->addPendingVB(bucketId, 0);
}

/*
    Resume flushing/persistence for the specified engine
*/
void StoragePool::resumeFlushing(bucket_id_t bucketId) {
    LockHolder lockHolder(engineMapLock);
    bucketsPaused.erase(bucketId);
    lockHolder.unlock();

    // Force the flusher todo something.
    // Setting a flush of vb:0 will force it to at least see if any
    // pendingPaused VBs need service.
    for (auto &shard : shards) {
        if (shard.get() != nullptr) {
            shard.get()->getFlusher()->addPendingVB(bucketId, 0);
        }
    }
}

/*
    Pause flushing/persistence for the specified engine
*/
void StoragePool::pauseFlushing(bucket_id_t bucketId)  {
    LockHolder lockHolder(engineMapLock);
    bucketsPaused.insert(bucketId);
}


void StoragePool::removeEngine(EventuallyPersistentEngine* engine) {
    LockHolder lh(engineMapLock);
    engineMap.erase(engine->getBucketId());
}

bool StoragePool::isFlushingPaused(bucket_id_t bucketId)  {
    LockHolder lockHolder(engineMapLock);
    return bucketsPaused.count(bucketId) != 0;
}

/*
    Basic factory that returns one storage pool.

    Future: support many storage pools.
*/
StoragePool& StoragePool::getStoragePool() {
    if (thePool == nullptr) {
        thePool = new StoragePool();
    }
    return *thePool;
}

void StoragePool::shutdown() {
    delete thePool;
    thePool = nullptr;
}

StoragePool* StoragePool::thePool = nullptr;

/*
    Create the storage pool's tasks
*/
void StoragePool::createTasks() {

    ExecutorPool::get()->registerTaskable(taskable);

#if HAVE_JEMALLOC
    /* Only create the defragmenter task if we have an underlying memory
     * allocator which can facilitate defragmenting memory.
     */
    EventuallyPersistentEngine* engine = ObjectRegistry::getCurrentEngine();
    defragmenterTask = new DefragmenterTask(this,  engine->getServerApi()->alloc_hooks);
    ExecutorPool::get()->schedule(defragmenterTask, NONIO_TASK_IDX);
#endif
}

StoragePool::Position StoragePool::pauseResumeVisit(PauseResumeStoragePoolVisitor& visitor,
                              Position& start_pos)
{
    const size_t maxSize = hashTableStorage.size();

    uint16_t vbid = start_pos.vbucket_id;
    for (; vbid < maxSize; ++vbid) {

        if (hashTableStorage[vbid].get()) {
            bool paused = !visitor.visit(vbid, *(hashTableStorage[vbid].get()));
            if (paused) {
                break;
            }
        }
    }

    return StoragePool::Position(vbid);
}

StoragePool::Position StoragePool::startPosition() const
{
    return StoragePool::Position(0);
}

StoragePool::Position StoragePool::endPosition() const
{
    return StoragePool::Position(hashTableStorage.size());
}

void StoragePool::runDefragmenterTask() {
    defragmenterTask->run();
}

std::ostream& operator<<(std::ostream& os,
                         const StoragePool::Position& pos) {
    os << "vbucket:" << pos.vbucket_id;
    return os;
}
