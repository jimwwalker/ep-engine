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

/**
 * Suite of performance tests for ep-engine.
 *
 * Uses the same engine_testapp infrastructure as ep_testsuite.
 *
 * Tests print their performance metrics to stdout; to see this output when
 * run via do:
 *
 *     make test ARGS="--verbose"
 *
**/

#include "config.h"

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#include "ep_testsuite_common.h"
#include "ep_test_apis.h"

struct Stats {
    std::string name;
    double mean;
    double median;
    double stddev;
    double pct5;
    double pct95;
    double pct99;
    std::vector<hrtime_t>* vec;
};

// Given a vector of timings (each a vector<hrtime_t>) calcuate metrics on them
// and print to stdout.
void print_timings(std::vector<std::pair<std::string, std::vector<hrtime_t>*> > timings)
{
    // First, calculate mean, median, standard deviation and percentiles of
    // each set of timings, both for printing and to derive what the range of
    // the graphs should be.
   //int(spark_start/1e3), int(spark_end/1e3));
}


/*****************************************************************************
 ** Testcases
 *****************************************************************************/

static enum test_result perf_latency(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1,
                                     const char* title) {
    // Only timing front-end performance, not considering persistence.
   // stop_persistence(h, h1);

    const void *cookie = testHarness.create_cookie();

    const unsigned int num_docs = 500000;
    const std::string data(100, 'x');

   // std::vector<hrtime_t> add_timings, get_timings, replace_timings, delete_timings;
   // add_timings.reserve(num_docs);
   // get_timings.reserve(num_docs);
   // replace_timings.reserve(num_docs);
  //  delete_timings.reserve(num_docs);

    int printed = 0;
    printf("\n\n=== Latency [%s] - %u items (µs) %n", title, num_docs, &printed);
    for (int i = 0; i < 81-printed; i++) {
        putchar('=');
    }

    int j = 0;

    // Build vector of keys
    std::vector<std::string> keys;

    for (unsigned int i = 0; i < num_docs; i++) {
        std::stringstream ss;
        ss << "KEY" << i;
        keys.push_back(ss.str());
    }

    // Create (add)
    for (std::vector<std::string>::iterator it = keys.begin(); it != keys.end(); ++it) { //auto& key : keys) {

        j++;
        item* item = NULL;
        const hrtime_t start = gethrtime();
        check(storeCasVb11(h, h1, cookie, OPERATION_ADD, it->c_str(),
                           data.c_str(), data.length(), 0, &item, 0,
                           /*vBucket*/0, 0, 0) == ENGINE_SUCCESS,
              "Failed to add a value");
        const hrtime_t end = gethrtime();
       // add_timings.push_back(end - start);
        h1->release(h, cookie, item);
        if (j==10000) {
            j=0;
            int items = get_int_stat(h, h1, "ep_diskqueue_items");
            //std::cout << "DWQ " << items << std::endl;
        }
    }
    j  = 0;
    // Update (Replace)
    for (std::vector<std::string>::iterator it = keys.begin(); it != keys.end(); ++it) { //auto& key : keys) {
       j++;
        item* item = NULL;
        const hrtime_t start = gethrtime();
        check(storeCasVb11(h, h1, cookie, OPERATION_REPLACE, it->c_str(),
                           data.c_str(), data.length(), 0, &item, 0,
                           /*vBucket*/0, 0, 0) == ENGINE_SUCCESS,
              "Failed to replace a value");
        const hrtime_t end = gethrtime();
       // replace_timings.push_back(end - start);
        h1->release(h, cookie, item);
        if (j==10000) {
            j=0;
            int items = get_int_stat(h, h1, "ep_diskqueue_items");
          //  std::cout << "DWQ " << items << std::endl;
        }
    }
    j  = 0;
    // Delete
     for (std::vector<std::string>::iterator it = keys.begin(); it != keys.end(); ++it) { //auto& key : keys) {
//   for (auto& key : keys) {
        j++;

        const hrtime_t start = gethrtime();
        check(del(h, h1, it->c_str(), 0, 0, cookie) == ENGINE_SUCCESS,
              "Failed to delete a value");
        const hrtime_t end = gethrtime();
     //   delete_timings.push_back(end - start);

        if (j==10000) {
            j=0;
            int items = get_int_stat(h, h1, "ep_diskqueue_items");
            //std::cout << "DWQ " << items << std::endl;
        }
    }
 wait_for_flusher_to_settle(h, h1);
  //  std::vector<std::pair<std::string, std::vector<hrtime_t>*> > all_timings;
  //  all_timings.push_back(std::make_pair("Add", &add_timings));
  //  all_timings.push_back(std::make_pair("Get", &get_timings));
  //  all_timings.push_back(std::make_pair("Replace", &replace_timings));
   // all_timings.push_back(std::make_pair("Delete", &delete_timings));
  //  print_timings(all_timings);

    return SUCCESS;
}

/* Benchmark the baseline latency (without any tasks running) of ep-engine.
 */
static enum test_result perf_latency_baseline(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    return perf_latency(h, h1, "Baseline");
}

/* Benchmark the baseline latency with the defragmenter enabled.
 */
static enum test_result perf_latency_defragmenter(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    return perf_latency(h, h1, "With constant defragmention");
}


/*****************************************************************************
 * List of testcases
 *****************************************************************************/

TestCase testsuite_testcases[] = {
        TestCase("Baseline latency", perf_latency_baseline,
                 test_setup, teardown,
                 "ht_size=393209", prepare, cleanup),
        TestCase("Defragmenter latency", perf_latency_defragmenter,
                 test_setup, teardown,
                 "ht_size=393209"
                 // Run defragmenter constantly.
                 ";defragmenter_interval=0",
                 prepare, cleanup),

        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup)
};
