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

#include "storeddockey.h"

#include <map>

class StoredDocKeyTest : public ::testing::TestWithParam<DocNamespace> {
};

class SerialisedDocKeyTest : public ::testing::TestWithParam<DocNamespace> {
};

class StoredDocKeyTestCombi : public ::testing::TestWithParam<std::tuple<DocNamespace, DocNamespace>> {
};

TEST_P(StoredDocKeyTest, constructor) {
    StoredDocKey key("key", GetParam());
    EXPECT_EQ(strlen("key"), key.size());
    EXPECT_EQ(0, std::memcmp("key", key.data(), sizeof("key")));
    EXPECT_EQ(GetParam(), key.getDocNamespace());
}

TEST_P(StoredDocKeyTest, copy_constructor) {
    StoredDocKey key1("key1", GetParam());
    StoredDocKey key2(key1);

    //exterally check rather than just use ==
    EXPECT_EQ(key1.size(), key2.size());
    EXPECT_EQ(key1.getDocNamespace(), key2.getDocNamespace());
    EXPECT_NE(key1.data(), key2.data());// must be different pointers
    EXPECT_TRUE(std::memcmp(key1.data(), key2.data(), key1.size()) == 0);
    EXPECT_EQ(key1, key2);
}

TEST_P(StoredDocKeyTest, assignment) {
    StoredDocKey key1("key1", GetParam());
    StoredDocKey key2("anotherkey", GetParam());

    key1 = key2;

    //exterally check
    EXPECT_EQ(key1.size(), key2.size());
    EXPECT_EQ(key1.getDocNamespace(), key2.getDocNamespace());
    EXPECT_NE(key1.data(), key2.data());// must be different pointers
    EXPECT_TRUE(std::memcmp(key1.data(), key2.data(), key1.size()) == 0);
    EXPECT_EQ(key1, key2);
}

TEST_P(StoredDocKeyTest, cStringSafe) {
    uint8_t raw[5] = {1,2,3,4,5};
    StoredDocKey key(raw, sizeof(raw), GetParam());
    EXPECT_EQ(5, strlen(reinterpret_cast<const char*>(key.data())));
    EXPECT_EQ(5, key.size());
}

TEST_P(StoredDocKeyTestCombi, equalityOperators) {
    StoredDocKey key1("key1", std::get<0>(GetParam()));
    StoredDocKey key2("key1", std::get<1>(GetParam()));
    StoredDocKey key3("key3", std::get<0>(GetParam()));
    StoredDocKey key4("key3", std::get<1>(GetParam()));

    EXPECT_TRUE(key1 != key3);
    EXPECT_TRUE(key2 != key4);
    if (std::get<0>(GetParam()) == std::get<1>(GetParam())) {
        EXPECT_TRUE(key1 == key2);
        EXPECT_FALSE(key1 != key2);
    } else {
        EXPECT_FALSE(key1 == key2);
        EXPECT_TRUE(key1 != key2);
    }
}

TEST_P(StoredDocKeyTestCombi, lessThan) {
    StoredDocKey key1("zzb", std::get<0>(GetParam()));
    StoredDocKey key2(key1);
    EXPECT_FALSE(key1 < key2); // same key

    StoredDocKey key1_ns1("zzb", std::get<1>(GetParam()));
    StoredDocKey key2_ns1(key1_ns1);
    EXPECT_FALSE(key1_ns1 < key2_ns1); // same key

    StoredDocKey key3("zza::thing", std::get<0>(GetParam()));
    StoredDocKey key3_ns1("zza::thing", std::get<1>(GetParam()));

    if (std::get<0>(GetParam()) < std::get<1>(GetParam())) {
        // DocNamespace is compared first, so if it is less all compares will
        // be less
        EXPECT_TRUE(key3 < key1_ns1);
        EXPECT_TRUE(key3 < key2_ns1);
        EXPECT_TRUE(key3 < key3_ns1);
        EXPECT_TRUE(key1 < key1_ns1);
        EXPECT_TRUE(key1 < key2_ns1);
        EXPECT_TRUE(key2 < key2_ns1);
        EXPECT_TRUE(key2 < key1_ns1);
    } else if(std::get<0>(GetParam()) == std::get<1>(GetParam())) {
        // Same namespace, so it's a check of key
        EXPECT_FALSE(key1 < key1_ns1);
        EXPECT_TRUE(key3 < key1);
        EXPECT_TRUE(key3 < key1_ns1);
    }
    else {
        EXPECT_FALSE(key3 < key1_ns1);
        EXPECT_FALSE(key3 < key2_ns1);
        EXPECT_FALSE(key3 < key3_ns1);
        EXPECT_FALSE(key1 < key1_ns1);
        EXPECT_FALSE(key1 < key2_ns1);
        EXPECT_FALSE(key2 < key2_ns1);
        EXPECT_FALSE(key2 < key1_ns1);
    }
}

// Test that the StoredDocKey can be used in std::map
TEST_P(StoredDocKeyTestCombi, map) {
    std::map<StoredDocKey, int> map;
    StoredDocKey key1("key1", std::get<0>(GetParam()));
    StoredDocKey key2("key2", std::get<0>(GetParam()));

    StoredDocKey key1_ns1("key1", std::get<1>(GetParam()));
    StoredDocKey key2_ns1("key2", std::get<1>(GetParam()));

    EXPECT_EQ(0, map.count(key1));
    EXPECT_EQ(0, map.count(key1_ns1));
    EXPECT_EQ(0, map.count(key2));
    EXPECT_EQ(0, map.count(key2_ns1));

    map[key1] = 1;
    map[key1_ns1] = 101;

    if (std::get<0>(GetParam()) == std::get<1>(GetParam())) {
        EXPECT_EQ(1, map.size());
        EXPECT_EQ(1, map.count(key1));
        EXPECT_TRUE(map[key1] == 101);
    } else {
        EXPECT_EQ(2, map.size());
        EXPECT_EQ(1, map.count(key1));
        EXPECT_EQ(1, map.count(key1_ns1));
        EXPECT_TRUE(map[key1] == 1);
        EXPECT_TRUE(map[key1_ns1] == 101);
    }

    map[key2] = 2;
    map[key2_ns1] = 102;


    if (std::get<0>(GetParam()) == std::get<1>(GetParam())) {
        EXPECT_EQ(2, map.size());
        EXPECT_EQ(1, map.count(key1));
        EXPECT_EQ(1, map.count(key2));
        EXPECT_TRUE(map[key1] == 101);
        EXPECT_TRUE(map[key2] == 102);
    } else {
        EXPECT_EQ(4, map.size());
        EXPECT_EQ(1, map.count(key1));
        EXPECT_EQ(1, map.count(key1_ns1));
        EXPECT_EQ(1, map.count(key2));
        EXPECT_EQ(1, map.count(key2_ns1));
        EXPECT_TRUE(map[key1] == 1);
        EXPECT_TRUE(map[key1_ns1] == 101);
        EXPECT_TRUE(map[key2] == 2);
        EXPECT_TRUE(map[key2_ns1] == 102);
    }
}

TEST_P(SerialisedDocKeyTest, constructor) {
    StoredDocKey key("key", GetParam());
    auto serialKey = SerialisedDocKey::make(key);
    EXPECT_EQ(sizeof("key"), serialKey->size());
    EXPECT_EQ(0, std::memcmp("key", serialKey->data(), sizeof("key")));
    EXPECT_EQ(GetParam(), serialKey->getDocNamespace());
}

TEST_P(StoredDocKeyTest, constructFromSerialisedDocKey) {
    StoredDocKey key1("key", GetParam());
    auto serialKey = SerialisedDocKey::make(key1);
    StoredDocKey key2(*serialKey);

    // Check key2 equals key1
    EXPECT_EQ(key1, key2);

    // Key1 equals serialKey
    EXPECT_EQ(key1, *serialKey);

    // Key 2 must equal serialKey (compare size, data, namespace)
    EXPECT_EQ(serialKey->size(), key2.size());
    EXPECT_EQ(0, std::memcmp("key", key2.data(), sizeof("key")));
    EXPECT_EQ(serialKey->getDocNamespace(), key2.getDocNamespace());
}

std::vector<DocNamespace> allDocNamespaces = {{DocNamespace::DefaultCollection,
                                               DocNamespace::Collections,
                                               DocNamespace::System}};

INSTANTIATE_TEST_CASE_P(DocNamespace, StoredDocKeyTestCombi,
                        ::testing::Combine(::testing::ValuesIn(allDocNamespaces),
                        ::testing::ValuesIn(allDocNamespaces)),);

INSTANTIATE_TEST_CASE_P(DocNamespace, StoredDocKeyTest,
                        ::testing::Values(DocNamespace::DefaultCollection,
                                          DocNamespace::Collections,
                                          DocNamespace::System),);

INSTANTIATE_TEST_CASE_P(DocNamespace, SerialisedDocKeyTest,
                        ::testing::Values(DocNamespace::DefaultCollection,
                                          DocNamespace::Collections,
                                          DocNamespace::System),);
