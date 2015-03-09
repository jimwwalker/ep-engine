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
    An abstract baseclass for objects which are to be ran as tasks.
*/

#pragma once

#include "workload.h"
#include "priority.h"
#include "platform/platform.h"
#include <stdint.h>

class Taskable {
public:
    /*
        Return a name for the task, used for logging
    */
    virtual std::string getName() const = 0;

    /*
        Return a 'group' ID for the task.

        The use-case is to identify all tasks belonging to an object.
        So the casted address of the owning object is a safe GID.
    */
    virtual uintptr_t getGID() const = 0;

    /*
        Return the workload priority for the task
    */
    virtual bucket_priority_t getWorkloadPriority() const = 0;

    /*
        Set the taskable object's workload priority.
    */
    virtual void setWorkloadPriority(bucket_priority_t prio) = 0;

    /*
        Return the taskable object's workload policy.
    */
    virtual WorkLoadPolicy& getWorkLoadPolicy() = 0;

    /*
        Called with the time spent queued
    */
    virtual void logQTime(type_id_t taskType, hrtime_t enqTime) = 0;

    /*
        Called with the time spent running
    */
    virtual void logRunTime(type_id_t taskType, hrtime_t runTime) = 0;
};
