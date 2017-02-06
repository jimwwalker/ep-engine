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

/**
 * Tests for Collection functionality in EPStore.
 */
#include "bgfetcher.h"
#include "tests/mock/mock_global_task.h"
#include "tests/module_tests/evp_store_test.h"

class CollectionsTest : public EPBucketTest {
    void SetUp() override {
        // Enable namespace persistence
        config_string += "persist_doc_namespace=true";
        EPBucketTest::SetUp();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active, false);
    }
};

TEST_F(CollectionsTest, namespace_separation) {
    store_item(vbid,
               {"$collections::create:meat 1", DocNamespace::DefaultCollection},
               "value");
    RCPtr<VBucket> vb = store->getVBucket(vbid);
    // Add the meat collection
    vb->updateFromManifest(
            {"{\"revision\":1, "
             "\"separator\":\"::\",\"collections\":[\"$default\",\"meat\"]}"});
    // Trigger a flush to disk. Flushes the meat create event and 1 item
    flush_vbucket_to_disk(vbid, 2);

    // evict and load - should not see the system key for create collections
    evict_key(vbid,
              {"$collections::create:meat 1", DocNamespace::DefaultCollection});
    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    GetValue gv = store->get(
            {"$collections::create:meat 1", DocNamespace::DefaultCollection},
            vbid,
            cookie,
            options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    // Manually run the BGFetcher task; to fetch the two outstanding
    // requests (for the same key).
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

    gv = store->get(
            {"$collections::create:meat 1", DocNamespace::DefaultCollection},
            vbid,
            cookie,
            options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ(0,
              strncmp("value",
                      gv.getValue()->getData(),
                      gv.getValue()->getNBytes()));
    delete gv.getValue();
}

TEST_F(CollectionsTest, collections_basic) {
    // Default collection is open for business
    store_item(vbid, {"key", DocNamespace::DefaultCollection}, "value");
    store_item(vbid,
               {"meat::beef", DocNamespace::Collections},
               "value",
               0,
               cb::engine_errc::unknown_collection);

    RCPtr<VBucket> vb = store->getVBucket(vbid);

    // Add the meat collection
    vb->updateFromManifest(
            {"{\"revision\":1, "
             "\"separator\":\"::\",\"collections\":[\"$default\",\"meat\"]}"});

    // Trigger a flush to disk. Flushes the meat create event and 1 item
    flush_vbucket_to_disk(vbid, 2);

    // Now we can write to beef
    store_item(vbid, {"meat::beef", DocNamespace::Collections}, "value");

    flush_vbucket_to_disk(vbid, 1);

    // And read a document from beef
    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    GetValue gv = store->get(
            {"meat::beef", DocNamespace::Collections}, vbid, cookie, options);
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
    delete gv.getValue();

    // A key in meat that doesn't exist
    gv = store->get({"meat::sausage", DocNamespace::Collections},
                    vbid,
                    cookie,
                    options);
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    // Begin the deletion
    vb->updateFromManifest(
            {"{\"revision\":2, "
             "\"separator\":\"::\",\"collections\":[\"$default\"]}"});

    // flush the delete event through
    flush_vbucket_to_disk(vbid, 1);

    // Access denied (although the item still exists)
    gv = store->get(
            {"meat::beef", DocNamespace::Collections}, vbid, cookie, options);
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION, gv.getStatus());
}
