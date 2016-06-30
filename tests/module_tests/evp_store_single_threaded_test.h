/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "evp_store_test.h"
#include "fakes/fake_executorpool.h"

/*
 * A subclass of EventuallyPersistentStoreTest which uses a fake ExecutorPool,
 * which will not spawn ExecutorThreads and hence not run any tasks
 * automatically in the background. All tasks must be manually run().
 */
class SingleThreadedEPStoreTest : public EventuallyPersistentStoreTest {
    void SetUp() {
        SingleThreadedExecutorPool::replaceExecutorPoolWithFake();
        EventuallyPersistentStoreTest::SetUp();

        task_executor = reinterpret_cast<SingleThreadedExecutorPool*>
            (ExecutorPool::get());
    }

    void TearDown() {
        shutdownAndPurgeTasks();
        EventuallyPersistentStoreTest::TearDown();
    }
protected:

    /*
     * Run the next task from the taskQ
     * The task must match the expectedTaskName parameter
     */
    void runNextTask(TaskQueue& taskQ, const std::string& expectedTaskName) {
        CheckedExecutor executor(task_executor, taskQ);

        // Run the task
        executor.runCurrentTask(expectedTaskName);
    }

    /*
     * Run the next task from the taskQ
     */
    void runNextTask(TaskQueue& taskQ) {
        CheckedExecutor executor(task_executor, taskQ);

        // Run the task
        executor.runCurrentTask();
    }

    /*
     * Change the vbucket state and run the VBStatePeristTask
     * On return the state will be changed and the task completed.
     */
    void setVBucketStateAndRunPersistTask(uint16_t vbid, vbucket_state_t newState) {
        auto& lpWriterQ = *task_executor->getLpTaskQ()[WRITER_TASK_IDX];

        // Change state - this should add 1 VBStatePersistTask to the WRITER queue.
        EXPECT_EQ(ENGINE_SUCCESS,
                  store->setVBucketState(vbid, newState, /*transfer*/false));

        runNextTask(lpWriterQ, "Persisting a vbucket state for vbucket: "
                               + std::to_string(vbid));
    }

    /*
     * Set the stats isShutdown and attempt to drive all tasks to cancel
     */
    void shutdownAndPurgeTasks() {
        engine->getEpStats().isShutdown = true;
        task_executor->cancelAll();

        for (task_type_t t :
             {WRITER_TASK_IDX, READER_TASK_IDX, AUXIO_TASK_IDX, NONIO_TASK_IDX}) {

            // Define a lambda to drive all tasks from the queue, if hpTaskQ
            // is implemented then trivial to add a second call to runTasks.
            auto runTasks = [=](TaskQueue& queue) {
                while (queue.getFutureQueueSize() > 0 || queue.getReadyQueueSize()> 0){
                    runNextTask(queue);
                }
            };
            runTasks(*task_executor->getLpTaskQ()[t]);
        }
    }

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(vbucket_failover_t* entry,
                                                   size_t nentries,
                                                   const void *cookie) {
        return ENGINE_SUCCESS;
    }

    SingleThreadedExecutorPool* task_executor;
};