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
#include <unordered_set>
#include <stdint.h>

#include "storagepool_shard.h"
#include "configuration.h"

class HashTable;
class HashTableStorage;
class EventuallyPersistentEngine;
class StoragePool;
class DefragmenterTask;

class StoragePoolTaskable : public Taskable {
public:

    StoragePoolTaskable(StoragePool* s)
        :
        myPool(s),
        prio(LOW_BUCKET_PRIORITY),
        needConfig(4, 4) {
    }

    std::string getName() const {
        return "StoragePool";
    }

    uintptr_t getGID() const {
        return reinterpret_cast<uintptr_t>(myPool);
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
    StoragePool* myPool;
    bucket_priority_t prio;
    WorkLoadPolicy needConfig; // TYNSET: Ideally from a config option.
};

/**
 * Base class for visiting a StoragePool with pause/resume support.
 */
class PauseResumeStoragePoolVisitor {
public:
    virtual ~PauseResumeStoragePoolVisitor() {}

    /**
     * Visit a hashtable within a storagepool.
     *
     * @param vbucket_id ID of the vbucket being visited.
     * @param ht a reference to the hashtable.
     * @return True if visiting should continue, otherwise false.
     */
    virtual bool visit(uint16_t vbucket_id, HashTableStorage& ht) = 0;
};


class StoragePool {
public:
    /**
     * Represents a position within the pool, used when visiting items.
     *
     * Currently opaque (and constant), clients can pass them around but
     * cannot reposition the iterator.
     */
    class Position {
    public:
        bool operator==(const Position& other) const {
            return (vbucket_id == other.vbucket_id);
        }

    private:
        Position(uint16_t vbucket_id_) : vbucket_id(vbucket_id_) {}

        uint16_t vbucket_id;

        friend class StoragePool;
        friend std::ostream& operator<<(std::ostream& os, const Position& pos);
    };

    StoragePool();
    ~StoragePool();

    /*
        Obtain the pools configuration setting
    */
    Configuration& getConfiguration();

    /*
        Create a new engine (bucket)
    */
    EventuallyPersistentEngine* createEngine(GET_SERVER_API get_server_api);

    /*
        Create a new HashTable to be used by the bucket/vbucket
    */
    HashTable& getHashTable(bucket_id_t bucketId, uint16_t vbid);

    /*
        Obtain a reference to the StoragePoolShard who will flush/fetch
        the specified vb (vbid)
    */
    StoragePoolShard& getStoragePoolShard(uint16_t vbid);

    /*
        Wake shard 0's flusher for flush-all for the bucket.
    */
    void wakeFlusherForFlushAll(bucket_id_t bucketId);

    /*
        Return the engine handle of the bucket id (or nullptr if not found)
    */
    EventuallyPersistentEngine* getEngine(bucket_id_t id);

    /*
        Called when an engine is going away, ensures global flusher completes work
    */
    void engineShuttingDown(EventuallyPersistentEngine* engine);

    /*
        Resume flushing/persistence for the specified bucket
    */
    void resumeFlushing(bucket_id_t bucketId);

    /*
        Pause flushing/persistence for the specified bucket
    */
    void pauseFlushing(bucket_id_t bucketId);

    /*
        Is flushing/persistence paused for the specified bucket?
    */
    bool isFlushingPaused(bucket_id_t bucketId);

    StoragePoolTaskable& getTaskable();

    /**
     * Visit the items in this storagepool, starting the iteration from the
     * given startPosition and allowing the visit to be paused at any point.
     *
     * During visitation, the visitor object can request that the visit
     * is stopped after the current item. The position passed to the
     * visitor can then be used to restart visiting at the *APPROXIMATE*
     * same position as it paused.
     * This is approximate as various locks are released when the
     * function returns, so any changes to the underlying epStore may cause
     * the visiting to restart at the slightly different place.
     *
     * As a consequence, *DO NOT USE THIS METHOD* if you need to guarantee
     * that all items are visited!
     *
     * @param visitor The visitor object.
     * @return The final storagepool position visited; equal to
     *         StoragePool::end() if all items were visited
     *         otherwise the position to resume from.
     */
    Position pauseResumeVisit(PauseResumeStoragePoolVisitor& visitor,
                              Position& start_pos);


    /**
     * Return a position at the start of the storage.
     */
    Position startPosition() const;

    /**
     * Return a position at the end of the storage. Has similar semantics
     * as STL end() (i.e. one past the last element).
     */
    Position endPosition() const;

    /**
     * Run the defragmenter and return once it's finished
     */
    void runDefragmenterTask();

    static StoragePool& getStoragePool();
    static void shutdown();

private:

    /*
        FUTURE: many storage pools...
    */
    static StoragePool* thePool;

    void removeEngine(EventuallyPersistentEngine* engine);
    void createTasks(EventuallyPersistentEngine* engine);

    Configuration config;
    std::vector< std::unique_ptr<HashTableStorage> > hashTableStorage;

    /*
        Storage pool provides flushing and fetching.
        Chunks of VBuckets are flushed and fetched by a shard.
    */
    std::vector< std::unique_ptr<StoragePoolShard> > shards;

    Mutex engineMapLock;
    std::unordered_map<bucket_id_t, EventuallyPersistentEngine*> engineMap;
    std::unordered_set<bucket_id_t> bucketsPaused;

    /*
        A pool owns a single defragmenter
    */
    DefragmenterTask* defragmenterTask;

    /*
        The storagepool will create tasks and requires a Taskable implementation
    */
    StoragePoolTaskable taskable;

    bool needToCreateTasks;


};
