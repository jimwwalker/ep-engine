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

#include "storagekey.h"

#include <map>
#include <unordered_map>

TEST(StorageKeyTest, constructor) {
    StorageKey key("key", sizeof("key"), StorageMetaFlag::DefaultCollection);
    EXPECT_EQ(1, sizeof(std::underlying_type<StorageMetaFlag>::type));
    EXPECT_EQ(sizeof("key") +
              sizeof(std::underlying_type<StorageMetaFlag>::type),
              key.size());
    EXPECT_EQ(StorageMetaFlag::DefaultCollection, key.getMetaFlag());
    EXPECT_EQ(sizeof("key"), key.getProtocolKey().size());
    EXPECT_EQ(0, std::memcmp("key", key.getProtocolKey().data(), sizeof("key")));
    EXPECT_NE(0, std::memcmp("key", key.data(), sizeof("key")));
}

TEST(StorageKeyTest, equalityOperators) {
    StorageKey key1("key1", sizeof("key1"), StorageMetaFlag::DefaultCollection);
    StorageKey key2("key1", sizeof("key1"), StorageMetaFlag::DefaultCollection);
    StorageKey key3("key1", sizeof("key1"), StorageMetaFlag::Collection);
    StorageKey key4("key1", sizeof("key1"), StorageMetaFlag::Collection);
    EXPECT_TRUE(key1 == key2);
    EXPECT_TRUE(key3 == key4);
    EXPECT_FALSE(key1 == key3);
    EXPECT_FALSE(key3 == key2);

    EXPECT_FALSE(key1 != key2);
    EXPECT_FALSE(key3 != key4);
    EXPECT_TRUE(key1 != key3);
    EXPECT_TRUE(key3 != key2);
}

TEST(StorageKeyTest, lessThan) {
    StorageKey key1("zzb", sizeof("zzb"), StorageMetaFlag::DefaultCollection);
    StorageKey key2("zzb", sizeof("zzb"), StorageMetaFlag::DefaultCollection);
    StorageKey key3("zza::thing", sizeof("zza::thing"), StorageMetaFlag::DefaultCollection);

    EXPECT_FALSE(key1 < key2);
    EXPECT_FALSE(key1 < key3);
    EXPECT_FALSE(key2 < key1);
    EXPECT_TRUE(key3 < key2);
}

TEST(SerialisedStorageKeyTest, constructor) {
    auto key = SerialisedStorageKey::make("key", sizeof("key"), StorageMetaFlag::DefaultCollection);
    EXPECT_EQ(sizeof("key") +
              sizeof(std::underlying_type<StorageMetaFlag>::type),
              key->size());
    EXPECT_EQ(StorageMetaFlag::DefaultCollection, key->getMetaFlag());
    EXPECT_EQ(sizeof("key"), key->getProtocolKey().size());
    EXPECT_EQ(0, std::memcmp("key", key->getProtocolKey().data(), sizeof("key")));
    EXPECT_NE(0, std::memcmp("key", key->data(), sizeof("key")));
}

TEST(StorageKeyTest, constructFromSerialisedStorageKey) {
    auto serialKey = SerialisedStorageKey::make("key", sizeof("key"), StorageMetaFlag::Collection);
    StorageKey key(*serialKey.get());

    EXPECT_EQ(serialKey->size(),
              key.size());
    EXPECT_EQ(StorageMetaFlag::Collection, key.getMetaFlag());
    EXPECT_EQ(sizeof("key"), key.getProtocolKey().size());
    EXPECT_EQ(0, std::memcmp("key", key.getProtocolKey().data(), sizeof("key")));
    EXPECT_NE(0, std::memcmp("key", key.data(), sizeof("key")));
}

// Test that the StorageKey can be used in std::map
TEST(StorageKeyTest, map) {
    std::map<StorageKey, int> map;
    StorageKey key1("key1", sizeof("key1"), StorageMetaFlag::DefaultCollection);
    StorageKey key3("key1", sizeof("key1"), StorageMetaFlag::Collection);

    EXPECT_EQ(0, map.count(key1));
    map[key1] = 8;
    EXPECT_EQ(1, map.count(key1));
    EXPECT_TRUE(map[key1] == 8);

    EXPECT_EQ(0, map.count(key3));
    map[key3] = 121;
    EXPECT_EQ(1, map.count(key3));
    EXPECT_TRUE(map[key3] == 121);
}

// Test that the StorageKey can be used in std::unordered_map
TEST(StorageKeyTest, unordered_map) {
    std::unordered_map<StorageKey, int> map;
    StorageKey key1("key1", sizeof("key1"), StorageMetaFlag::DefaultCollection);
    StorageKey key3("key1", sizeof("key1"), StorageMetaFlag::Collection);

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

TEST(StorageKeyTest, noheap) {
    const char* mykey = "this_is_my_key";
    StorageKeyNoHeap key1 = StorageKeyNoHeap(mykey, sizeof(mykey));

    EXPECT_EQ(mykey, key1.data());
    EXPECT_EQ(sizeof(mykey), key1.size());
    EXPECT_NE(0, std::memcmp("this_is_my_key", key1.getProtocolKey().data(), sizeof("this_is_my_key")));
    EXPECT_EQ(0, std::memcmp("this_is_my_key", key1.data(), sizeof("this_is_my_key")));
}

TEST(StorageKeyTest, protocolKey) {
    StorageKey key1("key1andjunk", 4, StorageMetaFlag::Collection);

    EXPECT_EQ(4, key1.getProtocolKey().size());

    // StorageKey is safe to assume a C string via data()
    EXPECT_EQ(4, strlen(key1.getProtocolKey().data()));
}