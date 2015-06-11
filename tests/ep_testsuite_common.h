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

#ifndef TESTS_EP_TESTSUITE_COMMON_H_
#define TESTS_EP_TESTSUITE_COMMON_H_

#include "config.h"

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#define WHITESPACE_DB "whitespace sucks.db"

#ifdef __cplusplus
extern "C" {
#endif

/* API required by engine_testapp to be able to drive a testsuite. */
MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void);

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th);

MEMCACHED_PUBLIC_API
bool teardown_suite(void);

#ifdef __cplusplus
}
#endif


class TestCase {
public:
    TestCase(const char *_name,
             enum test_result(*_tfun)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *),
             bool(*_test_setup)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *),
             bool(*_test_teardown)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *),
             const char *_cfg,
             enum test_result (*_prepare)(engine_test_t *test),
             void (*_cleanup)(engine_test_t *test, enum test_result result),
             bool _skip = false);

    TestCase(const TestCase &o);

    const char *getName() {
        return name;
    }

    engine_test_t *getTest();

private:
    engine_test_t test;
    const char *name;
    const char *cfg;
    bool skip;
};

// Name to use for database directory
extern const char *dbname_env;

// Handle of the test_harness, provided by engine_testapp.
extern struct test_harness testHarness;

enum test_result rmdb(void);

// Default testcase setup function.
bool test_setup(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);

// Default testcase teardown function.
bool teardown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);

// Default testcase prepare function.
enum test_result prepare(engine_test_t *test);

// Default testcase cleanup function.
void cleanup(engine_test_t *test, enum test_result result);


#endif /* TESTS_EP_TESTSUITE_COMMON_H_ */
