#include "storagepool_flusher.h"
#include "flusher.h" // TYNSET: only for RETRY_FLUSH_VBUCKET (-1)
#include "vbucket.h"
#include "executorpool.h"
#include "ep_engine.h"
#include "storagepool.h"

bool StoragePoolFlusherTask::run() {
    bool rv = false;
    snooze(flusher.run(rv));
    return rv;
}

StoragePoolFlusher::StoragePoolFlusher(StoragePool& sp, StoragePoolShardTaskable& spt)
    : state(FLUSH), storagePool(sp), taskable(spt), flusherSleepTime(0), wakeupTime(0) {
}

void StoragePoolFlusher::start() {
    ExTask task = new StoragePoolFlusherTask(&taskable, *this, Priority::FlusherPriority, 0.0, false);
    taskId = task->getId();
    ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
}

void StoragePoolFlusher::stop() {
    ExecutorPool::get()->cancel(taskId);

}

void StoragePoolFlusher::wake() {
    ExecutorPool::get()->wake(taskId);
}


void StoragePoolFlusher::addPendingVB(bucket_id_t id, uint16_t vb) {
    if (!storagePool.isFlushingPaused(id)) {
        LockHolder lockHolder(pendingMutex);
        pending[id].insert(vb);
        wake();
    } else {
        LockHolder lockHolder(pendingMutex);
        pendingPaused[id].insert(vb);
    }
}

/*
    Flush the engine with the set of vbucket ids.
    @param engine The engine to flush
    @param vbuckets A set of vbucket IDs which are pending a flush
    @param lockHolder A reference to the *locked* lockHolder protecting the set
*/
void StoragePoolFlusher::flushOneBucket(EventuallyPersistentEngine* engine, std::set<uint16_t>& vbuckets, LockHolder& lockHolder) {
    auto iter = vbuckets.begin();

    // iterate the set of VB ids.
    while(iter != vbuckets.end()) {
        uint16_t vbid = *iter;
        iter = vbuckets.erase(iter);

        // drop the lock to reduce I/O path contention when marking VBs dirty
        lockHolder.unlock();

        // set the engine for memory tracking
        EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(engine, true);
        int flushed = engine->getEpStore()->flushVBucket(vbid);
        ObjectRegistry::onSwitchThread(epe, false);

        lockHolder.lock();
        if(flushed == RETRY_FLUSH_VBUCKET) {
            // put the VB back in the std::set
            vbuckets.insert(vbid);
        } else {
            setupCheckpointPending(engine, vbid);
        }
    }
}

//
// If the VB has someone waiting for notification about a checkpoint or specific seqno
// then the flusher can't sleep forever incase the waiter needs timing out.
// Find the nearest wakeup and sleep for that many seconds.
//
void StoragePoolFlusher::setupCheckpointPending(EventuallyPersistentEngine* engine, uint16_t vbid) {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    if (vb && vb->getHighPriorityChkSize() > 0) {

        rel_time_t nextWakeup = vb->findNextCheckpointWakeup() - wakeupTime;

        if (nextWakeup < flusherSleepTime) {
            flusherSleepTime = nextWakeup == 0 ? 1 : nextWakeup;
        }
        checkpointPending[engine->getBucketId()].insert(vbid);
    }
}

void StoragePoolFlusher::flushAllBuckets() {
    LockHolder lockHolder(pendingMutex);

    auto pendingBucketIter = pending.begin();
    while (pendingBucketIter != pending.end()) {
        EventuallyPersistentEngine* currentEngine = nullptr;

        if (!storagePool.isFlushingPaused(pendingBucketIter->first)) {

            // Check if this bucket has any paused VBs
            if (pendingPaused.count(pendingBucketIter->first)) {
                // bring them back over to the pending flush set.
                for (auto vbid : pendingPaused[pendingBucketIter->first]) {
                    pendingBucketIter->second.insert(vbid);
                }
                // and drop them from paused.
                pendingPaused.erase(pendingBucketIter->first);
            }

            if ((currentEngine = storagePool.getEngine(pendingBucketIter->first)) != nullptr) {
                flushOneBucket(currentEngine, pendingBucketIter->second, lockHolder);
            } else {
                pendingBucketIter->second.clear();
            }

            // If the set is now empty, drop the bucket from the pending map
            if (pendingBucketIter->second.size() == 0) {
                pendingBucketIter = pending.erase(pendingBucketIter);
            }

            // else it's not empty, means a bucket/VB became ready again, don't delete and try this bucket again
        } else {
            // flushing is off for the bucket
            // move the vbuckets to the pending-paused set so the flusher now ignores them
            for (auto vbid : pending[pendingBucketIter->first]) {
                    pendingPaused[pendingBucketIter->first].insert(vbid);
            }

            // remove bucket from pending
            pendingBucketIter = pending.erase(pendingBucketIter);
        }
    }
}

/*
    Request that the specific bucket is flushed.
    Function blocks until flusher has flushed that bucket.
*/
void StoragePoolFlusher::flushEngineAndWait(EventuallyPersistentEngine* engine) {
    LockHolder lockHolder(pendingMutex);

    // If the bucket has no pending mutations then no flushing is required.
    if (pending.count(engine->getBucketId()) == 0) {
        return;
    }

    // Move state to the special flush and notify status
    state = FLUSH_AND_NOTIFY_FOR_ENGINE;
    SyncObject syncOnceForEngine;
    LockHolder lock(syncOnceForEngine);
    engineQueue.push_back(std::make_pair(&syncOnceForEngine, engine));
    lockHolder.unlock();

    // wait for flusher to complete
    syncOnceForEngine.wait();
}

double StoragePoolFlusher::run(bool& runAgain) {
    double sleepTime = INT_MAX; // default sleep forever
    LockHolder lockHolder(pendingMutex);

    // Record the wakeup time in-case we need to compute a checkpoint sleep interval
    wakeupTime = ep_current_time();
    flusherSleepTime = INT_MAX;

    // Move any checkpoint pending VBs over to the pending list for processing by the main loop
    if (checkpointPending.size() > 0) {
        for (auto cpPend : checkpointPending) {
            pending[cpPend.first].insert(cpPend.second.begin(), cpPend.second.end());
        }
    }

    while (pending.size() > 0 && state != SHUTDOWN) {
        lockHolder.unlock();
        flushAllBuckets();
        lockHolder.lock();

        // The flush and notify state requires a flush and wakeup of a waiting thread
        if (state == FLUSH_AND_NOTIFY_FOR_ENGINE) {
            auto engine = engineQueue.back();

            if (pending.count(engine.second->getBucketId()) > 0) {
                flushOneBucket(engine.second, pending[engine.second->getBucketId()], lockHolder);
            }

            engine.first->notifyOne();
            engineQueue.pop_back();

            // Once the engineQueue is empty go back to normal flush
            if (engineQueue.size() == 0) {
                state = FLUSH;
            }
        }
    }

    // Only if there are connections waiting on a checkpoint command shall
    // we sleep for an interval
    if (checkpointPending.size() > 0) {
        sleepTime = getFlusherSleepTime();
    }

    runAgain = true;
    return sleepTime;
}