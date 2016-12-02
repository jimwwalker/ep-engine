/*
 *     Copyright 2016 Couchbase, Inc
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

#include <gtest/gtest.h>

#include "makestoreddockey.h"
#include "storeddockey.h"

#include <map>
#include <unordered_map>

TEST(StoredDocKeyTest, constructor) {
    StoredDocKey key = makeStoredDocKey("key");
    EXPECT_EQ(strlen("key"), key.size());
    EXPECT_EQ(0, std::memcmp("key", key.data(), sizeof("key")));
    EXPECT_EQ(DocNamespace::DefaultCollection, key.getDocNamespace());
}

TEST(StoredDocKeyTest, cStringSafe) {
    uint8_t raw[5] = {1,2,3,4,5};
    StoredDocKey key(raw, sizeof(raw), DocNamespace::DefaultCollection);
    EXPECT_EQ(5, strlen(reinterpret_cast<const char*>(key.data())));
    EXPECT_EQ(5, key.size());
}

TEST(StoredDocKeyTest, equalityOperators) {
    StoredDocKey key1 = makeStoredDocKey("key1");
    StoredDocKey key2 = makeStoredDocKey("key1");
    StoredDocKey key3 = makeStoredDocKey("key3");

    EXPECT_TRUE(key1 == key2);
    EXPECT_TRUE(key1 != key3);
}

TEST(StoredDocKeyTest, lessThan) {
    StoredDocKey key1 = makeStoredDocKey("zzb");
    StoredDocKey key2 = makeStoredDocKey("zzb");
    StoredDocKey key3 = makeStoredDocKey("zza::thing");

    EXPECT_FALSE(key1 < key2);
    EXPECT_FALSE(key1 < key3);
    EXPECT_FALSE(key2 < key1);
    EXPECT_TRUE(key3 < key2);
}

TEST(SerialisedDocKeyTest, constructor) {
    StoredDocKey key = makeStoredDocKey("key");
    auto serialKey = SerialisedDocKey::make(key);
    EXPECT_EQ(sizeof("key"), serialKey->size());
    EXPECT_EQ(0, std::memcmp("key", serialKey->data(), sizeof("key")));
}

TEST(StoredDocKeyTest, constructFromSerialisedDocKey) {
    StoredDocKey key1 = makeStoredDocKey("key");
    auto serialKey = SerialisedDocKey::make(key1);
    StoredDocKey key2(*serialKey);

    // Key 1 must be the same as key2
    EXPECT_EQ(key1, key2);

    // Key 2 must equal serialKey (compare size, data, namespace)
    EXPECT_EQ(serialKey->size(), key2.size());
    EXPECT_EQ(0, std::memcmp("key", key2.data(), sizeof("key")));
    EXPECT_EQ(serialKey->getDocNamespace(), key2.getDocNamespace());
}

// Test that the StoredDocKey can be used in std::map
TEST(StoredDocKeyTest, map) {
    std::map<StoredDocKey, int> map;
    StoredDocKey key1 = makeStoredDocKey("key1");
    StoredDocKey key3 = makeStoredDocKey("key2");

    EXPECT_EQ(0, map.count(key1));
    map[key1] = 8;
    EXPECT_EQ(1, map.count(key1));
    EXPECT_TRUE(map[key1] == 8);

    EXPECT_EQ(0, map.count(key3));
    map[key3] = 121;
    EXPECT_EQ(1, map.count(key3));
    EXPECT_TRUE(map[key3] == 121);
}

// Test that the StoredDocKey can be used in std::unordered_map
TEST(StoredDocKeyTest, unordered_map) {
    std::unordered_map<StoredDocKey, int> map;
    StoredDocKey key1 = makeStoredDocKey("key1");
    StoredDocKey key3 = makeStoredDocKey("key2");

    // map is empty, so no key1
    EXPECT_EQ(0, map.count(key1));

    // Add key1 and check again
    map[key1] = 8;
    EXPECT_EQ(1, map.count(key1));
    EXPECT_TRUE(map[key1] == 8);

    // key3 has yet to be added
    EXPECT_EQ(0, map.count(key3));

    // Add and check
    map[key3] = 121;
    EXPECT_EQ(1, map.count(key3));
    EXPECT_TRUE(map[key3] == 121);

    // Remap key1
    map[key1] = 1000;
    EXPECT_EQ(1, map.count(key1));
    EXPECT_TRUE(map[key1] == 1000);
}

TEST(StoredDocKeyTest, vector_of_keys) {
    // test code does this
    std::vector<StoredDocKey> keys;
    for (int i = 0; i < 10; i++) {
        keys.push_back(makeStoredDocKey(std::to_string(i)));
    }

    std::random_shuffle(keys.begin(), keys.end());
}