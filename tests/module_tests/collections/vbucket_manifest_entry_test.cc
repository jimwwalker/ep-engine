/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "collections/vbucket_manifest_entry.h"

#include <gtest/gtest.h>

TEST(ManifestEntry, test1) {
    std::string collection_name = "beer";
    Collections::VBucket::ManifestEntry m(
            {collection_name.data(), collection_name.size()}, 100, 1000);
    EXPECT_EQ(1000, m.getStartSeqno());
    EXPECT_EQ(StoredValue::state_collection_open, m.getEndSeqno());
    EXPECT_FALSE(m.isDeleting());
    EXPECT_EQ("beer", m.getCollectionName());
    EXPECT_EQ(strlen("beer"), m.getCharBuffer().size());
    EXPECT_STREQ("beer", m.getCharBuffer().data());
}

//
// A collection is represented as deleting when it's start seqno >= end
//
TEST(ManifestEntry, test_isDeleting) {
    std::string collection_name = "beer";
    Collections::VBucket::ManifestEntry m(
            {collection_name.data(), collection_name.size()}, 100, 1000);
    m.setEndSeqno(m.getStartSeqno());
    EXPECT_TRUE(m.isDeleting());
    m.setEndSeqno(m.getStartSeqno() + 1);
    EXPECT_TRUE(m.isDeleting());
}