/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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

#ifndef SRC_WORKLOAD_H_
#define SRC_WORKLOAD_H_ 1

#include "config.h"
#include <string>
#include "common.h"

typedef enum {
    HIGH_BUCKET_PRIORITY=6,
    LOW_BUCKET_PRIORITY=2,
    NO_BUCKET_PRIORITY=0
} bucket_priority_t;

typedef enum {
    READ_HEAVY,
    WRITE_HEAVY,
    MIXED
} workload_pattern_t;

/**
 * Workload optimization policy
 */
class WorkLoadPolicy {
public:
    WorkLoadPolicy(int m, int s)
        : maxNumWorkers(m), maxNumShards(s), workloadPattern(READ_HEAVY) { }

    size_t getNumShards(void) {
        return maxNumShards;
    }

    bucket_priority_t getBucketPriority(void) {
        if (maxNumWorkers < HIGH_BUCKET_PRIORITY) {
            return LOW_BUCKET_PRIORITY;
        }
        return HIGH_BUCKET_PRIORITY;
    }

    size_t getNumWorkers(void) {
        return maxNumWorkers;
    }

    workload_pattern_t getWorkLoadPattern(void) {
        return workloadPattern;
    }

    std::string stringOfWorkLoadPattern(void) {
        switch (workloadPattern) {
        case READ_HEAVY:
            return "read_heavy";
        case WRITE_HEAVY:
            return "write_heavy";
        default:
            return "mixed";
        }
    }

    void setWorkLoadPattern(workload_pattern_t pattern) {
        workloadPattern = pattern;
    }

private:

    int maxNumWorkers;
    int maxNumShards;
    volatile workload_pattern_t workloadPattern;
};

#endif  // SRC_WORKLOAD_H_
