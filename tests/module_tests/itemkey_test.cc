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

#include "config.h"
#include "itemkey.h"
#include <string.h>
#include <unordered_map>

typedef std::unordered_map<ItemKey, int, ItemKeyHash> ItemKeyMap;

static void testLength() {
    ItemKey k1("a", 1, 0);
    cb_assert(k1.getKeyLen() == 1);
    ItemKey k2("32_byte_key____________________A", 32, 0);
    cb_assert(k2.getKeyLen() == 32);
}

static void testZeroTermination() {
    // Debug code regularly prints the client key even though there's no 0
    // termination in the memcached spec.
    // ItemKey code should add 0 past the end of the key for safe printing.
    ItemKey k("keyprintablecharacters#####", 22, 99);
    cb_assert(k.getKey()[22] == 0);
}

static void testGetBucketID() {
    ItemKey k("keyprintablecharacters#####", 22, 99);
    cb_assert(k.getBucketId() == 99);
}

static void testHashKeyLength() {
    ItemKey k("keyprintablecharacters#####", 22, 99);
    cb_assert(k.getHashKeyLen() >= k.getKeyLen());
}

static void testCopyConstructor() {
    ItemKey k("keyprintablecharacters#####", 22, 99);
    ItemKey k1 = k;
    cb_assert(k1.getKeyLen() == 22);
    cb_assert(k1.getKey()[22] == 0);
    cb_assert(k1.getBucketId() == 99);
    cb_assert(k1.getHashKeyLen() >= k.getKeyLen());
    cb_assert(memcmp(k1.getKey(), k.getKey(), 22) == 0);
    cb_assert(memcmp(k1.getHashKey(), k.getHashKey(), k1.getHashKeyLen()) == 0);
}

static void testItemKeyHash() {
    ItemKey k1("KEY1", 4, 1);
    ItemKey k2("KEY1", 4, 2);
    ItemKey k3("KEY2", 4, 3);
    ItemKey k4 = k2;

    ItemKeyMap map;
    map[k1] = 1;
    map[k2] = 2;
    map[k3] = 3;

    cb_assert(map[k1] == 1);
    cb_assert(map[k2] == 2);
    cb_assert(map[k3] == 3);
    cb_assert(map[k4] == 2);
}

static void testComparisonOperators() {
    ItemKey k1("KEY1", 4, 1);
    ItemKey k2("KEY1", 4, 101);
    ItemKey k3("KEY11", 5, 101);
    ItemKey k4("KEY1", 4, 1);
    ItemKey k5 = k4;

    cb_assert(k1 != k2);
    cb_assert(k1 == k4);
    cb_assert(k1 == k5);
    cb_assert(k2 != k3);
    cb_assert(k2 < k3);
}

int main() {
    testLength();
    testZeroTermination();
    testGetBucketID();
    testHashKeyLength();
    testCopyConstructor();
    testItemKeyHash();
    testComparisonOperators();
    return 0;
}