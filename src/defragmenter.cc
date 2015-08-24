/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#include "defragmenter.h"

#include "defragmenter_visitor.h"
#include "ep_engine.h"
#include "stored-value.h"

DefragmenterTask::DefragmenterTask(StoragePool* s,
                                   ALLOCATOR_HOOKS_API* alloc)
  : GlobalTask(s->getTaskable(), Priority::DefragmenterTaskPriority, 0.0, false),
    store_position(s->startPosition()),
    visitor(NULL),
    my_pool(s),
    alloc_hooks(alloc) {
}

DefragmenterTask::~DefragmenterTask() {
    delete visitor;
}

bool DefragmenterTask::run(void) {
    if (my_pool->getConfiguration().isDefragmenterEnabled()) {
        // Get our visitor. If we didn't finish the previous pass,
        // then resume from where we last were, otherwise create a new visitor and
        // reset the position.
        if (visitor == NULL) {
            visitor = new DefragmentVisitor(getAgeThreshold());
            store_position = my_pool->startPosition();
        }

        // Print start status.
        std::stringstream ss;
        ss << getDescription() << " for pool ";
        if (store_position == my_pool->startPosition()) {
            ss << " starting. ";
        } else {
            ss << " resuming from " << store_position << ", ";
            ss << visitor->getHashtablePosition() << ".";
        }
        ss << " Using chunk_duration=" << getChunkDurationMS() << " ms."
           << ", mapped_bytes=" << getMappedBytes();
        LOG(EXTENSION_LOG_INFO, "%s", ss.str().c_str());

        // Disable thread-caching (as we are about to defragment, and hence don't
        // want any of the new Blobs in tcache).
        bool old_tcache = alloc_hooks->enable_thread_cache(false);

        // Prepare the visitor.
        hrtime_t start = gethrtime();
        hrtime_t deadline = start + (getChunkDurationMS() * 1000 * 1000);
        visitor->setDeadline(deadline);
        visitor->clearStats();

        // Do it - set off the visitor.
        store_position = my_pool->pauseResumeVisit
                (*visitor, store_position);
        hrtime_t end = gethrtime();

        // Defrag complete. Restore thread caching.
        alloc_hooks->enable_thread_cache(old_tcache);

        // Release any free memory we now have in the allocator back to the OS.
        // TODO: Benchmark this - is it necessary? How much of a slowdown does it
        // add? How much memory does it return?
        alloc_hooks->release_free_memory();

        // Check if the visitor completed a full pass.
        bool completed = (store_position == my_pool->endPosition());

        // Print status.
        ss.str("");
        ss << getDescription() << " for pool ";
        if (completed) {
            ss << " finished.";
        } else {
            ss << " paused at position " << store_position << ".";
        }
        ss << " Took " << (end - start) / 1024 << " us."
           << " moved " << visitor->getDefragCount() << "/"
           << visitor->getVisitedCount() << " visited documents."
           << ", mapped_bytes=" << getMappedBytes()
           << ". Sleeping for " << getSleepTime() << " seconds.";
        LOG(EXTENSION_LOG_INFO, "%s", ss.str().c_str());

        // Delete visitor if it finished.
        if (completed) {
            delete visitor;
            visitor = NULL;
        }
    }

    snooze(getSleepTime());
    return true;
}

void DefragmenterTask::stop(void) {
    if (taskId) {
        ExecutorPool::get()->cancel(taskId);
    }
}

std::string DefragmenterTask::getDescription(void) {
    return std::string("Memory defragmenter");
}

size_t DefragmenterTask::getSleepTime() const {
    return my_pool->getConfiguration().getDefragmenterInterval();
}

size_t DefragmenterTask::getAgeThreshold() const {
    return my_pool->getConfiguration().getDefragmenterAgeThreshold();
}

size_t DefragmenterTask::getChunkDurationMS() const {
    return my_pool->getConfiguration().getDefragmenterChunkDuration();
}

size_t DefragmenterTask::getMappedBytes() {
    allocator_stats stats = {0};
    stats.ext_stats_size = alloc_hooks->get_extra_stats_size();
    stats.ext_stats = new allocator_ext_stat[stats.ext_stats_size];
    alloc_hooks->get_allocator_stats(&stats);

    size_t mapped_bytes = stats.heap_size - stats.free_mapped_size -
                          stats.free_unmapped_size;
    delete[] stats.ext_stats;
    return mapped_bytes;
}
