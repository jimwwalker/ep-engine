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

/*
 * StoragePool flusher.
 *
 * A flusher task that services the vbuckets of many buckets.
 *
 * Designed to be fully async, i.e. sleeps until a bucket has mutations.
 */

#pragma once
#include <set>
#include <unordered_map>
#include "tasks.h"

class StoragePoolFlusher;
class StoragePool;
class StoragePoolShardTaskable;

#define RETRY_FLUSH_VBUCKET (-1)

/*
 * GlobalTask sub-class to schedule the main code
 */
class StoragePoolFlusherTask : public GlobalTask {
public:
    StoragePoolFlusherTask(Taskable* t, StoragePoolFlusher& f,
                            const Priority &p, double sleeptime, bool shutdown)
        : GlobalTask(t, p, sleeptime, shutdown), flusher(f) { }

    bool run();

    std::string getDescription() {
        return std::string("StoragePool item flusher");
    }

private:
    StoragePoolFlusher& flusher;
};

class StoragePoolFlusher {
public:
    StoragePoolFlusher(StoragePool& sp, StoragePoolShardTaskable& spt);

    /*
     * offtask
     * Start the storagepool flusher task
     */
    void start();

    /*
     * offtask
     * Stop the storagepool flusher task
     */
    void stop();

    /*
     * ontask
     * Run the storagepool task code.
     *
     * Returns the seconds the task should sleep for and runAgain is set to also
     * indicate if the task is to be re-scheduled.
     */
    double run(bool& runAgain);

    /*
     * offtask
     * Notify that the bucket:vbucket has pending mutation(s).
     */
    void addPendingVB(bucket_id_t id, uint16_t vb);

    /*
     * offtask
     * Notify that the bucket:vbucket has a checkpoint/seqno command
     * running (and probably blocking).
     */
    void addCheckpointPending(bucket_id_t id, uint16_t vb);

    /*
     * offtask
     * Force a flush of the specific VB.
     * The function blocks until complete.
     */
    void flushEngineAndWait(EventuallyPersistentEngine* engine);

private:

    enum State {
        // flush vbuckets as they become ready
        FLUSH,
        // flush vbuckets as they become ready *and* force a flush on specific
        // buckets and notify a waiter
        FLUSH_AND_NOTIFY_FOR_ENGINE,
        // flusher is shutting down. Empty the queues and stop the task.
        SHUTDOWN
        // TYNSET TODO: possibly need a shutdown force option.
    };

    /*
     * offtask
     * request that the global task code schedules the flusher task
     */
    void wake();

    /*
     * ontask
     * 1. flush every bucket marked as pending (by using flushOneBucket)
     * 2. check if a bucket is moving between pause/resume and manage the
     *    pendingPaused data
     */
    void flushAllBuckets();

    /*
     * ontask
     * Flush the specified engine's set of vbuckets.
     * Caller should pass a lockHolder that is locking pendingMutex
     */
    void flushOneBucket(EventuallyPersistentEngine* engine,
                        std::set<uint16_t>& vbuckets,
                        LockHolder& lockHolder);

    /*
     * ontask
     * Perform any logic required to allow the flusher to correctly
     * wakeup/timeout any callers using the checkpoint/seqno notification
     * commands.
     */
    void setupCheckpointPending(EventuallyPersistentEngine* engine,
                                uint16_t vbid);

    /*
     * ontask
     * Return how long the flusher should sleep for.
     * Generally the flusher sleeps forever unless there are mutations
     */
    double getFlusherSleepTime() {
        return flusherSleepTime;
    }

    /*
     * The flusher's current state.
     */
    State state;

    /*
     * The flushers taskId, allocated by starting the task.
     */
    size_t taskId;

    /*
     * A reference to the pool this flusher is working for.
     */
    StoragePool& storagePool;

    /*
     * A reference to the pool-shard the flusher is working for.
     */
    StoragePoolShardTaskable& taskable;

    /*
     * This mutex currently provides serial access to all STL maps, engineQueue
     * and changes to state.
     */
    Mutex flusherLock;

    /*
     * map a bucket to a set of VBs that are dirty and require flushing.
     *
     * use flusherLock for safe access.
     */
    std::unordered_map<bucket_id_t, std::set<uint16_t> > pending;

    /*
     * map a bucket to a set of VBs that are dirty and require flushing when
     * flushing is enabled for the bucket.
     *
     * use flusherLock for safe access.
     */
    std::unordered_map<bucket_id_t, std::set<uint16_t> > pendingPaused;

    /*
     * map a bucket to a set of VBs that have checkpoint or seqno
     * commands waiting for a notification.
     *
     * use flusherLock for safe access.
     */
    std::unordered_map<bucket_id_t, std::set<uint16_t> > checkpointPending;

    /*
     * A list of engines that are blocked waiting for a flushAll to complete -
     * part of flushEngineAndWait.
     *
     * use flusherLock for safe access.
     */
    std::vector<EventuallyPersistentEngine*> engineQueue;

    /*
     * Calculated each time the flusher runs, how long it should now sleep for.
     */
    double flusherSleepTime;

    /*
     * Updated each time the flusher is woken.
     * Seconds granularity (from ep_current_time).
     */
    rel_time_t wakeupTime;
};