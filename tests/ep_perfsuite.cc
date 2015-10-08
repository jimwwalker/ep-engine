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
 * Note this is designed as a relatively quick micro-benchmark suite; tests
 * are tuned to complete in <2 seconds to maintain the quick turnaround.
**/

#include "config.h"

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#include <random>
#include <algorithm>
#include <iterator>

#include "ep_testsuite_common.h"
#include "ep_test_apis.h"

#include "mock/mock_dcp.h"

template<typename T>
struct Stats {
    std::string name;
    double mean;
    double median;
    double stddev;
    double pct5;
    double pct95;
    double pct99;
    std::vector<T>* values;
};

// Given a vector of values (each a vector<T>) calcuate metrics on them
// and print to stdout.
template<typename T>
void print_values(std::vector<std::pair<std::string, std::vector<T>*> > values,
                  std::string unit)
{
    // First, calculate mean, median, standard deviation and percentiles of
    // each set of values, both for printing and to derive what the range of
    // the graphs should be.
    std::vector<Stats<T>> value_stats;
    for (const auto& t : values) {
        Stats<T> stats;
        stats.name = t.first;
        stats.values = t.second;
        std::vector<T>& vec = *t.second;

        // Calculate latency percentiles
        std::sort(vec.begin(), vec.end());
        stats.median = vec[(vec.size() * 50) / 100];
        stats.pct5 = vec[(vec.size() * 5) / 100];
        stats.pct95 = vec[(vec.size() * 95) / 100];
        stats.pct99 = vec[(vec.size() * 99) / 100];

        const double sum = std::accumulate(vec.begin(), vec.end(), 0.0);
        stats.mean = sum / vec.size();
        double accum = 0.0;
        std::for_each (vec.begin(), vec.end(), [&](const double d) {
            accum += (d - stats.mean) * (d - stats.mean);
        });
        stats.stddev = sqrt(accum / (vec.size() - 1));

        value_stats.push_back(stats);
    }

    // From these find the start and end for the spark graphs which covers the
    // a "reasonable sample" of each value set. We define that as from the 5th
    // to the 95th percentile, so we ensure *all* sets have that range covered.
    T spark_start = std::numeric_limits<T>::max();
    T spark_end = 0;
    for (const auto& stats : value_stats) {
        spark_start = (stats.pct5 < spark_start) ? stats.pct5 : spark_start;
        spark_end = (stats.pct95 > spark_end) ? stats.pct95 : spark_end;
    }

    printf("\n\n                                Percentile           \n");
    printf("  %-15s Median     95th     99th  Std Dev  Histogram of samples\n\n", "");
    // Finally, print out each set.
    for (const auto& stats : value_stats) {
        if (stats.median/1e6 < 1) {
            printf("%-15s %8.03f %8.03f %8.03f %8.03f  ",
                    stats.name.c_str(), stats.median/1e3, stats.pct95/1e3,
                    stats.pct99/1e3, stats.stddev/1e3);
        } else {
            printf("%-8s (x1e3) %8.03f %8.03f %8.03f %8.03f  ",
                    stats.name.c_str(), stats.median/1e6, stats.pct95/1e6,
                    stats.pct99/1e6, stats.stddev/1e6);
        }

        // Calculate and render Sparkline (requires UTF-8 terminal).
        const int nbins = 32;
        int prev_distance = 0;
        std::vector<size_t> histogram;
        for (unsigned int bin = 0; bin < nbins; bin++) {
            const T max_for_bin = (spark_end / nbins) * bin;
            auto it = std::lower_bound(stats.values->begin(),
                                       stats.values->end(),
                                       max_for_bin);
            const int distance = std::distance(stats.values->begin(), it);
            histogram.push_back(distance - prev_distance);
            prev_distance = distance;
        }

        const auto minmax = std::minmax_element(histogram.begin(), histogram.end());
        const size_t range = *minmax.second - *minmax.first + 1;
        const int levels = 8;
        for (const auto& h : histogram) {
            int bar_size = ((h - *minmax.first + 1) * (levels - 1)) / range;
            putchar('\xe2');
            putchar('\x96');
            putchar('\x81' + bar_size);
        }
        putchar('\n');
    }
    printf("%51s  %-14d %s %14d\n\n", "",
           int(spark_start/1e3), unit.c_str(), int(spark_end/1e3));
}

void fillLineWith(const char c, int spaces) {
    for (int i = 0; i < spaces; ++i) {
        putchar(c);
    }
}

/*****************************************************************************
 ** Testcases
 *****************************************************************************/

static void perf_latency_core(ENGINE_HANDLE *h,
                              ENGINE_HANDLE_V1 *h1,
                              int id,
                              int num_docs,
                              std::vector<hrtime_t> &add_timings,
                              std::vector<hrtime_t> &get_timings,
                              std::vector<hrtime_t> &replace_timings,
                              std::vector<hrtime_t> &delete_timings) {

    const void *cookie = testHarness.create_cookie();
    const std::string data(100, 'x');

    // Build vector of keys
    std::vector<std::string> keys;
    for (int i = 0; i < num_docs; i++) {
        keys.push_back(std::to_string(id) + std::to_string(i));
    }

    // Create (add)
    for (auto& key : keys) {
        item* item = NULL;
        const hrtime_t start = gethrtime();
        check(storeCasVb11(h, h1, cookie, OPERATION_ADD, key.c_str(),
                           data.c_str(), data.length(), 0, &item, 0,
                           /*vBucket*/0, 0, 0) == ENGINE_SUCCESS,
                           "Failed to add a value");
        const hrtime_t end = gethrtime();
        add_timings.push_back(end - start);
        h1->release(h, cookie, item);
    }

    // Get
    for (auto& key : keys) {
        item* item = NULL;
        const hrtime_t start = gethrtime();
        check(h1->get(h, cookie, &item, key.c_str(), key.size(), 0) == ENGINE_SUCCESS,
              "Failed to get a value");
        const hrtime_t end = gethrtime();
        get_timings.push_back(end - start);
        h1->release(h, cookie, item);
    }

    // Update (Replace)
    for (auto& key : keys) {
        item* item = NULL;
        const hrtime_t start = gethrtime();
        check(storeCasVb11(h, h1, cookie, OPERATION_REPLACE, key.c_str(),
                           data.c_str(), data.length(), 0, &item, 0,
                           /*vBucket*/0, 0, 0) == ENGINE_SUCCESS,
              "Failed to replace a value");
        const hrtime_t end = gethrtime();
        replace_timings.push_back(end - start);
        h1->release(h, cookie, item);
    }

    // Delete
    for (auto& key : keys) {
        const hrtime_t start = gethrtime();
        check(del(h, h1, key.c_str(), 0, 0, cookie) == ENGINE_SUCCESS,
              "Failed to delete a value");
        const hrtime_t end = gethrtime();
        delete_timings.push_back(end - start);
    }

    testHarness.destroy_cookie(cookie);
}

static enum test_result perf_latency(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1,
                                     const char* title) {

    const unsigned int num_docs = 100000;

    // Only timing front-end performance, not considering persistence.
    stop_persistence(h, h1);

    std::vector<hrtime_t> add_timings, get_timings, replace_timings, delete_timings;
    add_timings.reserve(num_docs);
    get_timings.reserve(num_docs);
    replace_timings.reserve(num_docs);
    delete_timings.reserve(num_docs);

    int printed = 0;
    printf("\n\n=== Latency [%s] - %u items (µs) %n", title, num_docs, &printed);
    fillLineWith('=', 88-printed);

    // run and measure on this thread.
    perf_latency_core(h, h1, 0, num_docs, add_timings, get_timings, replace_timings, delete_timings);

    std::vector<std::pair<std::string, std::vector<hrtime_t>*> > all_timings;
    all_timings.push_back(std::make_pair("Add", &add_timings));
    all_timings.push_back(std::make_pair("Get", &get_timings));
    all_timings.push_back(std::make_pair("Replace", &replace_timings));
    all_timings.push_back(std::make_pair("Delete", &delete_timings));
    print_values(all_timings, "µs");
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

/* Benchmark the baseline latency with the defragmenter enabled.
 */
static enum test_result perf_latency_expiry_pager(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    return perf_latency(h, h1, "With constant Expiry pager");
}

struct ThreadArguments {
    void reserve(int n) {
        add_timings.reserve(n);
        get_timings.reserve(n);
        replace_timings.reserve(n);
        delete_timings.reserve(n);
    }
    ENGINE_HANDLE* h;
    ENGINE_HANDLE_V1* h1;
    int id;
    int num_docs;
    std::vector<hrtime_t> add_timings;
    std::vector<hrtime_t> get_timings;
    std::vector<hrtime_t> replace_timings;
    std::vector<hrtime_t> delete_timings;
};

extern "C" {
    static void perf_latency_thread(void *arg) {
        ThreadArguments* threadArgs = static_cast<ThreadArguments*>(arg);
        // run and measure on this thread.
        perf_latency_core(threadArgs->h,
                          threadArgs->h1,
                          threadArgs->id,
                          threadArgs->num_docs,
                          threadArgs->add_timings,
                          threadArgs->get_timings,
                          threadArgs->replace_timings,
                          threadArgs->delete_timings);
    }
}

//
// Test performance of many buckets/threads
//
static enum test_result perf_latency_baseline_multi_thread_bucket(engine_test_t* test,
                                                                  int n_buckets,
                                                                  int n_threads,
                                                                  int num_docs) {
    if (n_buckets > n_threads) {
        // not supporting...
        fprintf(stderr, "Returning FAIL because n_buckets(%d) > n_threads(%d)\n",
                n_buckets, n_threads);
        return FAIL;
    }

    std::vector<BucketHolder> buckets;

    int printed = 0;
    printf("\n\n=== Latency (%d-bucket(s) %d-thread(s)) - %u items (µs) %n",
           n_buckets,
           n_threads,
           num_docs,
           &printed);

    fillLineWith('=', 88-printed);

    if (create_buckets(test->cfg, n_buckets, buckets) != n_buckets) {
        destroy_buckets(buckets);
        return FAIL;
    }

    for (int ii = 0; ii < n_buckets; ii++) {
        // re-use test_setup to wait for ready
        test_setup(buckets[ii].h, buckets[ii].h1);
        // Only timing front-end performance, not considering persistence.
        stop_persistence(buckets[ii].h, buckets[ii].h1);
    }

    std::vector<ThreadArguments> thread_args(n_threads);
    std::vector<cb_thread_t> threads(n_threads);

    // setup the arguments each thread will use.
    // just round robin allocate buckets to threads
    int bucket = 0;
    for (int ii = 0; ii < n_threads; ii++) {
        thread_args[ii].h = buckets[bucket].h;
        thread_args[ii].h1 = buckets[bucket].h1;
        thread_args[ii].reserve(num_docs);
        thread_args[ii].num_docs = num_docs;
        thread_args[ii].id = ii;
        if ((++bucket) == n_buckets) {
            bucket = 0;
        }
    }

    // Now drive bucket(s) from thread(s)
    for (int i = 0; i < n_threads; i++) {
        int r = cb_create_thread(&threads[i], perf_latency_thread, &thread_args[i], 0);
        cb_assert(r == 0);
    }

    for (int i = 0; i < n_threads; i++) {
        int r = cb_join_thread(threads[i]);
        cb_assert(r == 0);
    }

    // destroy the buckets and rm the db path
    for (int ii = 0; ii < n_buckets; ii++) {
        testHarness.destroy_bucket(buckets[ii].h, buckets[ii].h1, false);
        rmdb(buckets[ii].dbpath.c_str());
    }

    // For the results, bring all the bucket timings into a single array
    std::vector<std::pair<std::string, std::vector<hrtime_t>*> > all_timings;
    std::vector<hrtime_t> add_timings, get_timings, replace_timings, delete_timings;
    for (int ii = 0; ii < n_threads; ii++) {
        add_timings.insert(add_timings.end(),
                           thread_args[ii].add_timings.begin(),
                           thread_args[ii].add_timings.end());
        get_timings.insert(get_timings.end(),
                           thread_args[ii].get_timings.begin(),
                           thread_args[ii].get_timings.end());
        replace_timings.insert(replace_timings.end(),
                               thread_args[ii].replace_timings.begin(),
                               thread_args[ii].replace_timings.end());
        delete_timings.insert(delete_timings.end(),
                              thread_args[ii].delete_timings.begin(),
                              thread_args[ii].delete_timings.end());
        // done with these arrays now
        thread_args[ii].add_timings.clear();
        thread_args[ii].get_timings.clear();
        thread_args[ii].replace_timings.clear();
        thread_args[ii].delete_timings.clear();
    }
    all_timings.push_back(std::make_pair("Add", &add_timings));
    all_timings.push_back(std::make_pair("Get", &get_timings));
    all_timings.push_back(std::make_pair("Replace", &replace_timings));
    all_timings.push_back(std::make_pair("Delete", &delete_timings));
    print_values(all_timings, "µs");

    return SUCCESS;
}

static enum test_result perf_latency_baseline_multi_bucket_2(engine_test_t* test) {
    return perf_latency_baseline_multi_thread_bucket(test,
                                                     2, /* buckets */
                                                     2, /* threads */
                                                     10000/* documents */);
}

static enum test_result perf_latency_baseline_multi_bucket_4(engine_test_t* test) {
    return perf_latency_baseline_multi_thread_bucket(test,
                                                     4, /* buckets */
                                                     4, /* threads */
                                                     10000/* documents */);
}

enum class Doc_format {
    JSON_PADDED,
    JSON_RANDOM,
    BINARY_RANDOM
};

struct Handle_args {
    Handle_args(ENGINE_HANDLE *_h, ENGINE_HANDLE_V1 *_h1, int _count,
                Doc_format _type, std::string _name, uint32_t _opaque,
                uint16_t _vb, bool _getCompressed) :
        h(_h), h1(_h1), itemCount(_count), typeOfData(_type), name(_name),
        opaque(_opaque), vb(_vb), retrieveCompressed(_getCompressed)
    {
        timings.reserve(_count);
        bytes_received.reserve(_count);
    }

    Handle_args(struct Handle_args const &ha) :
        h(ha.h), h1(ha.h1), itemCount(ha.itemCount),
        typeOfData(ha.typeOfData), name(ha.name), opaque(ha.opaque),
        vb(ha.vb), retrieveCompressed(ha.retrieveCompressed),
        timings(ha.timings), bytes_received(ha.bytes_received)
    { }

    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    int itemCount;
    Doc_format typeOfData;
    std::string name;
    uint32_t opaque;
    uint16_t vb;
    bool retrieveCompressed;
    std::vector<hrtime_t> timings;
    std::vector<size_t> bytes_received;
};

std::vector<std::string> genVectorOfValues(Doc_format type,
                                           size_t count, size_t maxSize) {
    static const char alphabet[] =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789";

    size_t len = 0;

    std::random_device ran;
    std::default_random_engine dre(ran());
    std::uniform_int_distribution<> uid(0, sizeof(alphabet) - 2);

    std::vector<std::string> vals;
    vals.reserve(count);
    switch (type) {
        case Doc_format::JSON_PADDED:
            for (size_t i = 0; i < count; ++i) {
                len = ((i + 1) * 10) % maxSize; // Set field length
                len = (len == 0) ? 10 : len;    // Adjust field length
                std::string str(len, alphabet[i % (sizeof(alphabet) - 1)]);
                vals.push_back("{"
                               "\"one\":\"" + std::to_string(i) + "\", "
                               "\"two\":\"" + "TWO\", "
                               "\"three\":\"" + std::to_string(i) + "\", "
                               "\"four\":\"" + "FOUR\", "
                               "\"five\":\"" + str + "\""
                               "}");
            }
            break;
        case Doc_format::JSON_RANDOM:
            for (size_t i = 0; i < count; ++i) {
                len = ((i + 1) * 10) % maxSize; // Set field length
                len = (len == 0) ? 10 : len;    // Adjust field length
                std::string str;
                str.reserve(len);
                std::generate_n(std::back_inserter(str), len, [&]() {
                    return alphabet[uid(dre)];
                });
                vals.push_back("{"
                               "\"one\":\"" + std::to_string(i) + "\", "
                               "\"two\":\"" + str.substr(1, len * 0.003) + "\", "
                               "\"three\":\"" + str.substr(2, len * 0.001) + "\", "
                               "\"four\": \"" + str.substr(3, len * 0.002) + "\", "
                               "\"five\":\"" + str.substr(0, len * 0.05) + "\", "
                               "\"six\":\"" + "\"{1, 2, 3, 4, 5}\", "
                               "\"seven\":\"" + str.substr(4, len * 0.01) + "\", "
                               "\"eight\":\"" + str.substr(5, len * 0.01) + "\", "
                               "\"nine\":\"" + "{'abc', 'def', 'ghi'}\", "
                               "\"ten\":\"" + "0.123456789\", "
                               "}");
            }
            break;
        case Doc_format::BINARY_RANDOM:
            for (size_t i = 0; i < count; ++i) {
                len = ((i + 1) * 10) % maxSize; // Set field length
                len = (len == 0) ? 10 : len;    // Adjust field length
                std::string str;
                str.reserve(len);
                std::generate_n(std::back_inserter(str), len, [&]() {
                    return alphabet[uid(dre)];
                });
                vals.push_back(str);
            }
            break;
        default:
            check(false, "Unknown DATA requested!");
    }
    return vals;
}

static void perf_load_client(ENGINE_HANDLE *h,
                             ENGINE_HANDLE_V1 *h1,
                             uint16_t vbid,
                             int count,
                             Doc_format typeOfData,
                             std::vector<hrtime_t> &insertTimes) {

    item *it = NULL;
    std::vector<std::string> keys;
    std::vector<std::string> vals;
    for (int i = 0; i < count; ++i) {
        keys.push_back("key" + std::to_string(i));
    }
    vals = genVectorOfValues(typeOfData, count, 100000);

    for (int i = 0; i < count; ++i) {
        checkeq(store(h, h1, NULL, OPERATION_SET, keys[i].c_str(),
                      vals[i].c_str(), &it, 0, vbid),
                ENGINE_SUCCESS,
                "Failed set.");
        insertTimes.push_back(gethrtime());
        h1->release(h, NULL, it);
    }
    wait_for_flusher_to_settle(h, h1);
}

static void perf_dcp_client(struct Handle_args *ha) {
    const void *cookie = testHarness.create_cookie();

    uint64_t end = static_cast<uint64_t>(ha->itemCount);
    std::string uuid("vb_" + std::to_string(ha->vb) + ":0:id");
    uint64_t vb_uuid = get_ull_stat(ha->h, ha->h1, uuid.c_str(), "failovers");
    uint32_t streamOpaque = ha->opaque;

    checkeq(ha->h1->dcp.open(ha->h, cookie, ++streamOpaque, 0, DCP_OPEN_PRODUCER,
                             (void*)ha->name.c_str(), ha->name.length()),
            ENGINE_SUCCESS,
            "Failed dcp producer open connection");

    checkeq(ha->h1->dcp.control(ha->h, cookie, ++streamOpaque,
                                "connection_buffer_size",
                                strlen("connection_buffer_size"), "1024", 4),
            ENGINE_SUCCESS,
            "Failed to establish connection buffer");

    if (ha->retrieveCompressed) {
        checkeq(ha->h1->dcp.control(ha->h, cookie, ++streamOpaque,
                                    "enable_value_compression",
                                    strlen("enable_value_compression"), "true", 4),
                ENGINE_SUCCESS,
                "Failed to enable value compression");
    }

    uint64_t rollback = 0;
    checkeq(ha->h1->dcp.stream_req(ha->h, cookie, 0, streamOpaque,
                                   ha->vb, 0, end,
                                   vb_uuid, 0, 0, &rollback,
                                   mock_dcp_add_failover_log),
            ENGINE_SUCCESS,
            "Failed to initiate stream request");

    struct dcp_message_producers* producers = get_dcp_producers();

    bool done = false;
    uint32_t bytes_read = 0;
    bool pending_marker_ack = false;
    uint64_t marker_end = 0;

    size_t num_mutations = 0;

    do {
        if (bytes_read > 512) {
            ha->h1->dcp.buffer_acknowledgement(ha->h, cookie, ++streamOpaque,
                                               ha->vb, bytes_read);
            bytes_read = 0;
        }
        ENGINE_ERROR_CODE err = ha->h1->dcp.step(ha->h, cookie, producers);
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            switch (dcp_last_op) {
                case PROTOCOL_BINARY_CMD_DCP_MUTATION:
                    num_mutations++;
                    ha->timings.push_back(gethrtime());
                    ha->bytes_received.push_back(dcp_last_value.length());
                    bytes_read += dcp_last_packet_size;
                    if (pending_marker_ack && dcp_last_byseqno == marker_end) {
                        sendDcpAck(ha->h, ha->h1, cookie,
                                   PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                                   PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                   dcp_last_opaque);
                    }
                    break;
                case PROTOCOL_BINARY_CMD_DCP_STREAM_END:
                    done = true;
                    bytes_read += dcp_last_packet_size;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER:
                    if (dcp_last_flags & 8) {
                        pending_marker_ack = true;
                        marker_end = dcp_last_snap_end_seqno;
                    }
                    bytes_read += dcp_last_packet_size;
                    break;
                default:
                    break;
            }
            dcp_last_op = 0;
        }
    } while (!done);

    checkeq(num_mutations,
            static_cast<size_t>(ha->itemCount),
            "Didn't receive expected number of mutations");
    testHarness.destroy_cookie(cookie);
}

extern "C" {
    static void load_thread(void *args) {
        struct Handle_args *ha = static_cast<Handle_args *>(args);
        perf_load_client(ha->h,
                         ha->h1,
                         ha->vb,
                         ha->itemCount,
                         ha->typeOfData,
                         ha->timings);
    }

    static void dcp_client_thread(void *args) {
        struct Handle_args *ha = static_cast<Handle_args *>(args);
        perf_dcp_client(ha);
    }
}

struct Ret_vals {
    Ret_vals(struct Handle_args _ha, size_t n) :
        ha(_ha)
    {
        timings.reserve(n);
        received.reserve(n);
    }
    struct Handle_args ha;
    std::vector<hrtime_t> timings;
    std::vector<size_t> received;
};

static enum test_result perf_dcp_latency_and_bandwidth(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1,
                                                       std::string title,
                                                       Doc_format typeOfData,
                                                       size_t item_count) {

    std::vector<std::pair<std::string, std::vector<hrtime_t>*> > all_timings;
    std::vector<std::pair<std::string, std::vector<size_t>*> > all_sizes;

    std::vector<struct Ret_vals> iterations;

    // For Loader & DCP client to get documents as is from vbucket 0
    struct Handle_args ha1(h, h1, item_count, typeOfData, "As_is",
                           0xFFFFFF00, 0, false);
    struct Ret_vals rv1(ha1, item_count);
    iterations.push_back(rv1);

    // For Loader & DCP client to get documents compressed from vbucket 1
    struct Handle_args ha2(h, h1, item_count, typeOfData, "Compress",
                           0xFF000000, 1, true);
    struct Ret_vals rv2(ha2, item_count);
    iterations.push_back(rv2);

    for (size_t i = 0; i < iterations.size(); ++i) {
        std::vector<hrtime_t> timings;
        cb_thread_t loader_thread, dcp_thread;
        struct Handle_args load_ha(iterations[i].ha);
        struct Handle_args dcp_ha(iterations[i].ha);

        check(set_vbucket_state(h, h1, load_ha.vb, vbucket_state_active),
                "Failed set_vbucket_state for vbucket");
        wait_for_flusher_to_settle(h, h1);

        cb_assert(cb_create_thread(&loader_thread, load_thread, &load_ha, 0) == 0);
        cb_assert(cb_create_thread(&dcp_thread, dcp_client_thread, &dcp_ha, 0) == 0);
        cb_assert(cb_join_thread(loader_thread) == 0);
        cb_assert(cb_join_thread(dcp_thread) == 0);

        cb_assert(load_ha.timings.size() == dcp_ha.timings.size());

        for (size_t j = 0; j < load_ha.timings.size(); ++j) {
            if (load_ha.timings[j] < dcp_ha.timings[j]) {
                timings.push_back(dcp_ha.timings[j] - load_ha.timings[j]);
            } else {
                // Since there is no network overhead at all, it is seen
                // that sometimes the DCP client actually received the
                // mutation before the store from the load client returned
                // a SUCCESS.
                timings.push_back(0);
            }
        }
        iterations[i].timings = timings;
        iterations[i].received = dcp_ha.bytes_received;
        all_timings.push_back(std::make_pair(dcp_ha.name, &iterations[i].timings));
        all_sizes.push_back(std::make_pair(dcp_ha.name, &iterations[i].received));
    }

    int printed = 0;
    printf("\n\n=== %s Latency - %zu items (µs) %n",
           title.c_str(), item_count, &printed);
    fillLineWith('=', 88-printed);

    print_values(all_timings, "µs");

    printed = 0;
    printf("=== %s KB Rcvd. - %zu items (KB) %n",
           title.c_str(), item_count, &printed);
    fillLineWith('=', 86-printed);

    print_values(all_sizes, "KB");

    fillLineWith('=', 86);
    printf("\n\n");

    return SUCCESS;
}

static enum test_result perf_dcp_latency_with_padded_json(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    return perf_dcp_latency_and_bandwidth(h, h1,
                            "DCP In-memory (JSON-PADDED) [As_is vs. Compress]",
                            Doc_format::JSON_PADDED, 10000);
}

static enum test_result perf_dcp_latency_with_random_json(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    return perf_dcp_latency_and_bandwidth(h, h1,
                            "DCP In-memory (JSON-RAND) [As_is vs. Compress]",
                            Doc_format::JSON_RANDOM, 5000);
}

static enum test_result perf_dcp_latency_with_random_binary(ENGINE_HANDLE *h,
                                                            ENGINE_HANDLE_V1 *h1) {
    return perf_dcp_latency_and_bandwidth(h, h1,
                            "DCP In-memory (BINARY-RAND) [As_is vs. Compress]",
                            Doc_format::BINARY_RANDOM, 5000);
}

static enum test_result perf_multi_thread_latency(engine_test_t* test) {
    return perf_latency_baseline_multi_thread_bucket(test,
                                                     1, /* bucket */
                                                     4, /* threads */
                                                     10000/* documents */);
}


/*****************************************************************************
 * List of testcases
 *****************************************************************************/

/* NOTE: Please pass "backend=couchdb;dbname=./perf_test" as the test cfg
         parameter always. This ensures that the perf test doesn't not use the
         same file as the functional test when they are running concurrently. */
BaseTestCase testsuite_testcases[] = {
        TestCase("Baseline latency", perf_latency_baseline,
                 test_setup, teardown,
                 "backend=couchdb;dbname=./perf_test;ht_size=393209",
                 prepare, cleanup),
        TestCase("Defragmenter latency", perf_latency_defragmenter,
                 test_setup, teardown,
                 "backend=couchdb;dbname=./perf_test;ht_size=393209"
                 // Run defragmenter constantly.
                 ";defragmenter_interval=0",
                 prepare, cleanup),
        TestCase("Expiry pager latency", perf_latency_expiry_pager,
                 test_setup, teardown,
                 "backend=couchdb;dbname=./perf_test;ht_size=393209"
                 // Run expiry pager constantly.
                 ";exp_pager_stime=0",
                 prepare, cleanup),
        TestCaseV2("Multi bucket latency", perf_latency_baseline_multi_bucket_2,
                   NULL, NULL,
                   "backend=couchdb;dbname=./perf_test;ht_size=393209",
                   prepare, cleanup),
        TestCaseV2("Multi bucket latency", perf_latency_baseline_multi_bucket_4,
                   NULL, NULL,
                   "backend=couchdb;dbname=./perf_test;ht_size=393209",
                   prepare, cleanup),
        TestCase("DCP latency (Padded JSON)", perf_dcp_latency_with_padded_json,
                 test_setup, teardown,
                 "backend=couchdb;dbname=./perf_test;ht_size=393209",
                 prepare, cleanup),
        TestCase("DCP latency (Random JSON)", perf_dcp_latency_with_random_json,
                 test_setup, teardown,
                 "backend=couchdb;dbname=./perf_test;ht_size=393209",
                 prepare, cleanup),
        TestCase("DCP latency (Random BIN)", perf_dcp_latency_with_random_binary,
                 test_setup, teardown,
                 "backend=couchdb;dbname=./perf_test;ht_size=393209",
                 prepare, cleanup),
        TestCaseV2("Multi thread latency", perf_multi_thread_latency,
                   NULL, NULL,
                   "backend=couchdb;dbname=./perf_test;ht_size=393209",
                   prepare, cleanup),

        TestCase(NULL, NULL, NULL, NULL,
                 "backend=couchdb;dbname=./perf_test", prepare, cleanup)
};
