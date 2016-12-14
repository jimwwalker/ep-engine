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

#include "collections/manifest.h"
#include "collections/vbucket_manifest.h"
#include "tests/module_tests/makestoreddockey.h"

#include <gtest/gtest.h>

class VBucketManifestTest : public ::testing::Test { public:
    /**
        This function is emulating what I think we will do when receiving
        manifest from the cluster manager
    */
    void drive(const Collections::Manifest& manifest) {
        std::vector<std::string> add, del;
        static int64_t fakeSeqno = 0;
        vbucket_manifest.processManifest(manifest, add, del);

        // When integrated the fakeSeqno values become the seqno of meta-events
        // applied to the VB for each create_collection (add) and
        // delete_collection

        // Now apply additions
        for (auto collection : add) {
            vbucket_manifest.addCollection(collection,
                                           manifest.getRevision(),
                                           fakeSeqno++ /*startseq*/);
        }
        // Now apply deletions
        for (auto collection : del) {
            vbucket_manifest.beginDelCollection(collection,
                                                fakeSeqno++ /*endseq*/);
        }
    }

protected:
    Collections::VBucket::Manifest vbucket_manifest;
};

TEST_F(VBucketManifestTest, auto_iterate) {
    vbucket_manifest.addCollection("beer", 0, 0);
    vbucket_manifest.addCollection("lager", 0, 0);
    int count = 0;
    for (auto& e : vbucket_manifest) {
        (void)e;
        count++;
    }
    EXPECT_EQ(2, count);
    EXPECT_EQ(count, vbucket_manifest.size());
}

TEST_F(VBucketManifestTest, basic_exists) {
    vbucket_manifest.addCollection("beer", 0, 0);
    auto key1 = makeStoredDocKey("beer::bud", DocNamespace::Collections);
    EXPECT_TRUE(vbucket_manifest.collectionExists(
            Collections::DocKey::make(key1, "::")));
}

TEST_F(VBucketManifestTest, processManifest) {
    Collections::Manifest manifest(
            "{\"revision\":0,\"separator\":\":\","
            "\"collections\":[\"$default\",\"beer\",\"brewery\"]}");

    std::vector<std::string> add, del;
    vbucket_manifest.processManifest(manifest, add, del);
    EXPECT_EQ(manifest.size(), add.size());
    EXPECT_EQ(0, del.size());
}

TEST_F(VBucketManifestTest, addCollection_exception1) {
    Collections::Manifest manifest(
            "{\"revision\":0,"
            "\"separator\":\":\","
            "\"collections\":[\"$default\",\"beer\",\"brewery\"]}");

    std::vector<std::string> add, del;
    vbucket_manifest.processManifest(manifest, add, del);
    EXPECT_EQ(manifest.size(), add.size());
    EXPECT_EQ(0, del.size());

    // Now apply additions
    for (auto collection : add) {
        vbucket_manifest.addCollection(
                collection, manifest.getRevision(), 0 /*startseq*/);

        // If we added the collection again without beginning to delete or
        // completing delete...
        EXPECT_THROW(
                vbucket_manifest.addCollection(
                        collection, manifest.getRevision(), 0 /*startseq*/),
                std::logic_error);
    }
}

TEST_F(VBucketManifestTest, addCollection_noexception1) {
    Collections::Manifest manifest(
            "{\"revision\":0,"
            "\"separator\":\":\","
            "\"collections\":[\"$default\",\"beer\",\"brewery\"]}");

    std::vector<std::string> add, del;
    vbucket_manifest.processManifest(manifest, add, del);
    EXPECT_EQ(manifest.size(), add.size());
    EXPECT_EQ(0, del.size());

    // Now apply additions
    for (auto collection : add) {
        vbucket_manifest.addCollection(
                collection, manifest.getRevision(), 0 /*startseq*/);
        vbucket_manifest.beginDelCollection(collection, 1 /*endseqno*/);
        EXPECT_NO_THROW(vbucket_manifest.addCollection(
                collection, manifest.getRevision(), 2 /*startseq*/));
    }
}

TEST_F(VBucketManifestTest, addCollection_noexception2) {
    Collections::Manifest manifest(
            "{\"revision\":0,"
            "\"separator\":\":\","
            "\"collections\":[\"$default\",\"beer\",\"brewery\"]}");

    std::vector<std::string> add, del;
    vbucket_manifest.processManifest(manifest, add, del);
    EXPECT_EQ(manifest.size(), add.size());
    EXPECT_EQ(0, del.size());

    // Now apply additions
    for (auto collection : add) {
        vbucket_manifest.addCollection(
                collection, manifest.getRevision(), 0 /*startseq*/);
        vbucket_manifest.beginDelCollection(collection, 1 /*endseqno*/);
        vbucket_manifest.completeDelCollection(collection);
        EXPECT_NO_THROW(vbucket_manifest.addCollection(
                collection, manifest.getRevision(), 2 /*startseq*/));
    }
}

TEST_F(VBucketManifestTest, manifest_updates1) {
    Collections::Manifest manifest(
            "{\"revision\":0,"
            "\"separator\":\":\","
            "\"collections\":[\"$default\",\"beer\",\"brewery\"]}");

    std::vector<std::string> add, del;
    vbucket_manifest.processManifest(manifest, add, del);
    EXPECT_EQ(manifest.size(), add.size());
    EXPECT_EQ(0, del.size());

    // Now apply additions
    for (auto collection : add) {
        vbucket_manifest.addCollection(
                collection, manifest.getRevision(), 0 /*startseq*/);
    }

    add.clear();
    del.clear();

    // Expect no addition/deletions if we process again
    vbucket_manifest.processManifest(manifest, add, del);
    EXPECT_EQ(0, add.size());
    EXPECT_EQ(0, del.size());

    // New manifest, removes beer, so expect 0 add and 1 del
    Collections::Manifest manifest1(
            "{\"revision\":1,"
            "\"separator\":\":\","
            "\"collections\":[\"$default\",\"brewery\"]}");

    vbucket_manifest.processManifest(manifest1, add, del);
    EXPECT_EQ(0, add.size());
    EXPECT_EQ(1, del.size());

    // Now apply deletion
    vbucket_manifest.beginDelCollection(del[0], 1 /*endseqno*/);

    // New manifest, adds beer (again) and adds lager, so expect 2 add and 0 del
    Collections::Manifest manifest2(
            "{\"revision\":2,"
            "\"separator\":\":\","
            "\"collections\":[\"$default\", \"brewery\", \"beer\", "
            "\"lager\"]}");
    del.clear();
    vbucket_manifest.processManifest(manifest2, add, del);
    EXPECT_EQ(2, add.size());
    EXPECT_EQ(0, del.size());
}

TEST_F(VBucketManifestTest, manifest_updates2) {
    Collections::VBucket::Manifest vbucket_manifest;
    drive({"{\"revision\":0, \"separator\":\":\","
           "\"collections\":[\"$default\",\"beer\",\"brewery\"]}"});
    EXPECT_EQ(3, vbucket_manifest.size());
    drive({"{\"revision\":1, \"separator\":\":\","
           "\"collections\":[\"$default\",\"brewery\"]}"});
    // beer deletion begins... so 3 is correct
    EXPECT_EQ(3, vbucket_manifest.size());
    vbucket_manifest.completeDelCollection("beer");
    EXPECT_EQ(2, vbucket_manifest.size());
}

/**
 * Test the following:
 * 1. add beer, brewery
 * 2. begin deletion of beer (set a manifest with just brewery)
 * 3. add beer, brewery
 * 4. complete deletion of beer
 * 5. expect 2 collections exist, i.e. beer still exists
 */
TEST_F(VBucketManifestTest, manifest_updates3) {
    Collections::VBucket::Manifest vbucket_manifest;
    drive({"{\"revision\":0, \"separator\":\":\","
           "\"collections\":[\"beer\",\"brewery\"]}"});
    EXPECT_EQ(2, vbucket_manifest.size());
    drive({"{\"revision\":1, \"separator\":\":\","
           "\"collections\":[\"brewery\"]}"});
    // beer deletion begins... so 3 is correct
    EXPECT_EQ(2, vbucket_manifest.size());
    drive({"{\"revision\":3, \"separator\":\":\","
           "\"collections\":[\"beer\",\"brewery\"]}"});
    vbucket_manifest.completeDelCollection("beer");
    // completeDelCollection was ignored as beer was added back
    EXPECT_EQ(2, vbucket_manifest.size());
    // Verify that all collections are "open"
    for (auto& entry : vbucket_manifest) {
        EXPECT_FALSE(entry.second->isDeleting());
        EXPECT_EQ(StoredValue::state_collection_open,
                  entry.second->getEndSeqno());
    }
}