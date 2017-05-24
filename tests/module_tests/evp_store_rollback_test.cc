/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

/**
 * Tests for Rollback functionality in EPStore.
 */

#include "dcp/dcpconnmap.h"
#include "evp_store_single_threaded_test.h"
#include "evp_store_test.h"
#include "failover-table.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/module_tests/test_helpers.h"

class RollbackTest : public EPBucketTest,
                     public ::testing::WithParamInterface<std::string>
{
    void SetUp() override {
        EPBucketTest::SetUp();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active, false);

        // For any rollback tests which actually want to rollback, we need
        // to ensure that we don't rollback more than 50% of the seqno count
        // as then the VBucket is just cleared (it'll instead expect a resync
        // from zero.
        // Therefore create 10 dummy items which we don't otherwise care
        // about (most of the Rollback test only work with a couple of
        // "active" items.
        const auto dummy_elements = size_t{5};
        for (size_t ii = 1; ii <= dummy_elements; ii++) {
            auto res = store_item(vbid,
                                  makeStoredDocKey("dummy" + std::to_string(ii)),
                                  "dummy");
            ASSERT_EQ(ii, res.getBySeqno());
        }
        ASSERT_EQ(dummy_elements, store->flushVBucket(vbid));
        initial_seqno = dummy_elements;
    }

protected:
    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(vbucket_failover_t* entry,
                                                   size_t nentries,
                                                   const void *cookie) {
        return ENGINE_SUCCESS;
    }

    /**
     * Test rollback after deleting an item.
     * @param flush_before_rollback: Should the vbuckt be flushed to disk just
     *        before the rollback (i.e. guaranteeing the in-memory state is in sync
     *        with disk).
     */
    void rollback_after_deletion_test(bool flush_before_rollback) {
        // Setup: Store an item then flush the vBucket (creating a checkpoint);
        // then delete the item and create a second checkpoint.
        StoredDocKey a = makeStoredDocKey("key");
        auto item_v1 = store_item(vbid, a, "1");
        ASSERT_EQ(initial_seqno + 1, item_v1.getBySeqno());
        ASSERT_EQ(1, store->flushVBucket(vbid));
        uint64_t cas = item_v1.getCas();
        ASSERT_EQ(ENGINE_SUCCESS,
                  store->deleteItem(a,
                                    cas,
                                    vbid,
                                    /*cookie*/ nullptr,
                                    /*itemMeta*/ nullptr,
                                    /*mutation_descr_t*/ nullptr));
        if (flush_before_rollback) {
            ASSERT_EQ(1, store->flushVBucket(vbid));
        }
        // Sanity-check - item should no longer exist.
        EXPECT_EQ(ENGINE_KEY_ENOENT,
                  store->get(a, vbid, nullptr, {}).getStatus());

        // Test - rollback to seqno of item_v1 and verify that the previous value
        // of the item has been restored.
        store->setVBucketState(vbid, vbucket_state_replica, false);
        ASSERT_EQ(ENGINE_SUCCESS, store->rollback(vbid, item_v1.getBySeqno()));
        auto result = getInternal(
                a, vbid, /*cookie*/ nullptr, vbucket_state_replica, {});
        ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
        EXPECT_EQ(item_v1, *result.getValue())
            << "Fetched item after rollback should match item_v1";
        delete result.getValue();

        if (!flush_before_rollback) {
            EXPECT_EQ(0, store->flushVBucket(vbid));
        }
    }

    // Test rollback after modifying an item.
    void rollback_after_mutation_test(bool flush_before_rollback) {
        // Setup: Store an item then flush the vBucket (creating a checkpoint);
        // then update the item with a new value and create a second checkpoint.
        StoredDocKey a = makeStoredDocKey("a");
        auto item_v1 = store_item(vbid, a, "old");
        ASSERT_EQ(initial_seqno + 1, item_v1.getBySeqno());
        ASSERT_EQ(1, store->flushVBucket(vbid));

        auto item2 = store_item(vbid, a, "new");
        ASSERT_EQ(initial_seqno + 2, item2.getBySeqno());

        StoredDocKey key = makeStoredDocKey("key");
        store_item(vbid, key, "meh");

        if (flush_before_rollback) {
            EXPECT_EQ(2, store->flushVBucket(vbid));
        }

        // Test - rollback to seqno of item_v1 and verify that the previous value
        // of the item has been restored.
        store->setVBucketState(vbid, vbucket_state_replica, false);
        ASSERT_EQ(ENGINE_SUCCESS, store->rollback(vbid, item_v1.getBySeqno()));
        ASSERT_EQ(item_v1.getBySeqno(), store->getVBucket(vbid)->getHighSeqno());

        // a should have the value of 'old'
        {
            auto result = store->get(a, vbid, nullptr, {});
            ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
            EXPECT_EQ(item_v1, *result.getValue())
                << "Fetched item after rollback should match item_v1";
            delete result.getValue();
        }

        // key should be gone
        {
            auto result = store->get(key, vbid, nullptr, {});
            EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                << "A key set after the rollback point was found";
        }

        if (!flush_before_rollback) {
            // The rollback should of wiped out any keys waiting for persistence
            EXPECT_EQ(0, store->flushVBucket(vbid));
        }
    }

// This test triggers MSVC 'cl' to assert, a lot of time has been spent trying
// to tweak the code so it compiles, but no solution yet. Disabled for VS 2013
#if !defined(_MSC_VER) || _MSC_VER != 1800
    void rollback_to_middle_test(bool flush_before_rollback) {
        // create some more checkpoints just to see a few iterations
        // of parts of the rollback function.

        // need to store a certain number of keys because rollback
        // 'bails' if the rollback is too much.
        for (int i = 0; i < 6; i++) {
            store_item(vbid, makeStoredDocKey("key_" + std::to_string(i)), "dontcare");
        }
        // the roll back function will rewind disk to key7.
        auto rollback_item = store_item(vbid, makeStoredDocKey("key7"), "dontcare");
        ASSERT_EQ(7, store->flushVBucket(vbid));

        // every key past this point will be lost from disk in a mid-point.
        auto item_v1 = store_item(vbid, makeStoredDocKey("rollback-cp-1"), "keep-me");
        auto item_v2 = store_item(vbid, makeStoredDocKey("rollback-cp-2"), "rollback to me");
        store_item(vbid, makeStoredDocKey("rollback-cp-3"), "i'm gone");
        auto rollback = item_v2.getBySeqno(); // ask to rollback to here.
        ASSERT_EQ(3, store->flushVBucket(vbid));

        for (int i = 0; i < 3; i++) {
            store_item(vbid, makeStoredDocKey("anotherkey_" + std::to_string(i)), "dontcare");
        }

        if (flush_before_rollback) {
            ASSERT_EQ(3, store->flushVBucket(vbid));
        }


        // Rollback should succeed, but rollback to 0
        store->setVBucketState(vbid, vbucket_state_replica, false);
        EXPECT_EQ(ENGINE_SUCCESS, store->rollback(vbid, rollback));

        // These keys should be gone after the rollback
        for (int i = 0; i < 3; i++) {
            auto result = store->get(makeStoredDocKey("rollback-cp-" + std::to_string(i)), vbid, nullptr, {});
            EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                << "A key set after the rollback point was found";
        }

        // These keys should be gone after the rollback
        for (int i = 0; i < 3; i++) {
            auto result = store->get(makeStoredDocKey("anotherkey_" + std::to_string(i)), vbid, nullptr, {});
            EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                << "A key set after the rollback point was found";
        }

        // Rolled back to the previous checkpoint
        EXPECT_EQ(rollback_item.getBySeqno(),
                  store->getVBucket(vbid)->getHighSeqno());
    }
#endif

protected:
    int64_t initial_seqno;
};

TEST_P(RollbackTest, RollbackAfterMutation) {
    rollback_after_mutation_test(/*flush_before_rollbaack*/true);
}

TEST_P(RollbackTest, RollbackAfterMutationNoFlush) {
    rollback_after_mutation_test(/*flush_before_rollback*/false);
}

TEST_P(RollbackTest, RollbackAfterDeletion) {
    rollback_after_deletion_test(/*flush_before_rollback*/true);
}

TEST_P(RollbackTest, RollbackAfterDeletionNoFlush) {
    rollback_after_deletion_test(/*flush_before_rollback*/false);
}

#if !defined(_MSC_VER) || _MSC_VER != 1800
TEST_P(RollbackTest, RollbackToMiddleOfAPersistedSnapshot) {
    rollback_to_middle_test(true);
}

TEST_P(RollbackTest, RollbackToMiddleOfAPersistedSnapshotNoFlush) {
    rollback_to_middle_test(false);
}

TEST_P(RollbackTest, RollbackToMiddleOfAnUnPersistedSnapshot) {
    /* need to store a certain number of keys because rollback
       'bails (rolls back to 0)' if the rollback is too much. */
    const int numItems = 10;
    for (int i = 0; i < numItems; i++) {
        store_item(vbid,
                   makeStoredDocKey("key_" + std::to_string(i)),
                   "not rolled back");
    }

    /* the roll back function will rewind disk to key11. */
    auto rollback_item =
            store_item(vbid, makeStoredDocKey("key11"), "rollback pt");

    ASSERT_EQ(numItems + 1, store->flushVBucket(vbid));

    /* Keys to be lost in rollback */
    auto item_v1 = store_item(
            vbid, makeStoredDocKey("rollback-cp-1"), "hope to keep till here");
    /* ask to rollback to here; this item is in a checkpoint and
       is not persisted */
    auto rollbackReqSeqno = item_v1.getBySeqno();

    auto item_v2 = store_item(vbid, makeStoredDocKey("rollback-cp-2"), "gone");

    /* do rollback */
    store->setVBucketState(vbid, vbucket_state_replica, false);
    EXPECT_EQ(ENGINE_SUCCESS, store->rollback(vbid, rollbackReqSeqno));

    /* confirm that we have rolled back to the disk snapshot */
    EXPECT_EQ(rollback_item.getBySeqno(),
              store->getVBucket(vbid)->getHighSeqno());

    /* since we rely only on disk snapshots currently, we must lose the items in
       the checkpoints */
    for (int i = 0; i < 2; i++) {
        auto result =
                store->get(makeStoredDocKey("rollback-cp-" + std::to_string(i)),
                           vbid,
                           nullptr,
                           {});
        EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                << "A key set after the rollback point was found";
    }
}
#endif

/*
 * The opencheckpointid of a bucket is one after a rollback.
 */
TEST_P(RollbackTest, MB21784) {
    // Make the vbucket a replica
    store->setVBucketState(vbid, vbucket_state_replica, false);
    // Perform a rollback
    EXPECT_EQ(ENGINE_SUCCESS, store->rollback(vbid, initial_seqno))
        << "rollback did not return ENGINE_SUCCESS";

    // Assert the checkpointmanager clear function (called during rollback)
    // has set the opencheckpointid to one
    auto vb = store->getVBucket(vbid);
    auto& ckpt_mgr = vb->checkpointManager;
    EXPECT_EQ(1, ckpt_mgr.getOpenCheckpointId()) << "opencheckpointId not one";

    // Create a new Dcp producer, reserving its cookie.
    get_mock_server_api()->cookie->reserve(cookie);
    dcp_producer_t producer = engine->getDcpConnMap().newProducer(
            cookie, "test_producer", /*flags*/ 0, {/*no json*/});

    uint64_t rollbackSeqno;
    auto err = producer->streamRequest(/*flags*/0,
                                       /*opaque*/0,
                                       /*vbucket*/vbid,
                                       /*start_seqno*/0,
                                       /*end_seqno*/0,
                                       /*vb_uuid*/0,
                                       /*snap_start*/0,
                                       /*snap_end*/0,
                                       &rollbackSeqno,
                                       RollbackTest::fakeDcpAddFailoverLog);
    EXPECT_EQ(ENGINE_SUCCESS, err)
        << "stream request did not return ENGINE_SUCCESS";
    // Close stream
    ASSERT_EQ(ENGINE_SUCCESS, producer->closeStream(/*opaque*/0, vbid));
    engine->handleDisconnect(cookie);
}

class RollbackDcpTest : public SingleThreadedEPBucketTest {
public:
    RollbackDcpTest()
        : cookie(create_mock_cookie()),
          producers(get_dcp_producers(nullptr, nullptr)) {
    }

    void SetUp() override {
        SingleThreadedEPBucketTest::SetUp();
        store->setVBucketState(vbid, vbucket_state_active, false);
        consumer = new MockDcpConsumer(*engine, cookie, "test_consumer");
        vb = store->getVBucket(vbid);
        producers->stream_req = &RollbackDcpTest::streamRequest;
    }

    void TearDown() override {
        consumer->closeAllStreams();
        destroy_mock_cookie(cookie);
        consumer.reset();
        vb.reset();
        SingleThreadedEPBucketTest::TearDown();
    }

    // build a rollback response command
    std::unique_ptr<char[]> getRollbackResponse(uint32_t opaque,
                                                uint64_t rollbackSeq) const {
        auto msg = std::make_unique<char[]>(
                sizeof(protocol_binary_response_header) + sizeof(uint64_t));
        auto* p = reinterpret_cast<protocol_binary_response_dcp_stream_req*>(
                msg.get());

        p->message.header.response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
        p->message.header.response.status =
                htons(PROTOCOL_BINARY_RESPONSE_ROLLBACK);
        p->message.header.response.opaque = opaque;
        p->message.header.response.bodylen = htonl(sizeof(uint64_t));

        auto* seq = reinterpret_cast<uint64_t*>(p + 1);
        *seq = htonll(rollbackSeq);
        return msg;
    }

    static struct StreamRequestData {
        bool called;
        uint32_t opaque;
        uint16_t vbucket;
        uint32_t flags;
        uint64_t start_seqno;
        uint64_t end_seqno;
        uint64_t vbucket_uuid;
        uint64_t snap_start_seqno;
        uint64_t snap_end_seqno;
    } streamRequestData;

    static ENGINE_ERROR_CODE streamRequest(const void* cookie,
                                           uint32_t opaque,
                                           uint16_t vbucket,
                                           uint32_t flags,
                                           uint64_t start_seqno,
                                           uint64_t end_seqno,
                                           uint64_t vbucket_uuid,
                                           uint64_t snap_start_seqno,
                                           uint64_t snap_end_seqno) {
        streamRequestData = {true,
                             opaque,
                             vbucket,
                             flags,
                             start_seqno,
                             end_seqno,
                             vbucket_uuid,
                             snap_start_seqno,
                             snap_end_seqno};

        return ENGINE_SUCCESS;
    }

    void stepForStreamRequest(uint64_t startSeqno, uint64_t vbUUID) {
        while (consumer->step(producers.get()) == ENGINE_WANT_MORE) {
        }
        EXPECT_TRUE(streamRequestData.called);
        EXPECT_EQ(startSeqno, streamRequestData.start_seqno);
        EXPECT_EQ(vbUUID, streamRequestData.vbucket_uuid);
        streamRequestData = {};
    }

    void createItems(int items, int flushes) {
        // Flush multiple checkpoints of unique keys
        for (int ii = 0; ii < flushes; ii++) {
            EXPECT_TRUE(store_items(items,
                                    vbid,
                                    {"anykey_" + std::to_string(ii) + "_",
                                     DocNamespace::DefaultCollection},
                                    "value"));
            flush_vbucket_to_disk(vbid, items);
            // Add an entry for this seqno
            vb->failovers->createEntry(items * (ii + 1));
        }

        store->setVBucketState(vbid, vbucket_state_replica, false);
    }

    uint64_t addStream(int nitems) {
        consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0);
        // Step consumer to retrieve the first stream request.
        uint64_t vbUUID = vb->failovers->getLatestEntry().vb_uuid;
        stepForStreamRequest(nitems, vbUUID);
        return vbUUID;
    }

    void responseNoRollback(int nitems,
                            uint64_t rollbackSeq,
                            uint64_t previousUUID) {
        // Now push a reponse to the consumer, saying rollback to 0.
        // The consumer must ignore the 0 rollback and retry a stream-request
        // with the next failover entry.
        auto msg = getRollbackResponse(1 /*opaque*/, rollbackSeq);
        EXPECT_TRUE(consumer->handleResponse(
                reinterpret_cast<protocol_binary_response_header*>(msg.get())));

        // Consumer should of added a StreamRequest with a different vbuuid
        EXPECT_NE(previousUUID, vb->failovers->getLatestEntry().vb_uuid);

        stepForStreamRequest(nitems, vb->failovers->getLatestEntry().vb_uuid);
    }

    void responseRollback(uint64_t rollbackSeq) {
        // Now push a reponse to the consumer, saying rollback to 0.
        // The consumer must ignore the 0 rollback and retry a stream-request
        // with the next failover entry.
        auto msg = getRollbackResponse(1 /*opaque*/, rollbackSeq);
        EXPECT_TRUE(consumer->handleResponse(
                reinterpret_cast<protocol_binary_response_header*>(msg.get())));

        // consumer must of scheduled a RollbackTask (writer task)
        auto& lpWriteQ = *task_executor->getLpTaskQ()[WRITER_TASK_IDX];
        ASSERT_EQ(1, lpWriteQ.getFutureQueueSize());
        runNextTask(lpWriteQ);
    }

    const void* cookie;
    SingleThreadedRCPtr<MockDcpConsumer> consumer;
    std::unique_ptr<dcp_message_producers> producers;
    VBucketPtr vb;
};

RollbackDcpTest::StreamRequestData RollbackDcpTest::streamRequestData = {};

/**
 * Push stream responses to a consumer and test
 * 1. The first rollback to 0 response is ignored, the consumer requests again
 *    with new data.
 * 2. The second rollback to 0 response triggers a rollback to 0.
 */
TEST_F(RollbackDcpTest, test_rollback_zero) {
    const int items = 40;
    const int flushes = 1;
    const int nitems = items * flushes;
    const int rollbackPoint = 0; // expect final rollback to be to 0

    // Test will create anykey_0_{0..items-1}
    createItems(items, flushes);

    auto uuid = addStream(nitems);

    responseNoRollback(nitems, 0, uuid);

    // All keys available
    for (int ii = 0; ii < items; ii++) {
        std::string key = "anykey_0_" + std::to_string(ii);
        auto result = store->get(
                {key, DocNamespace::DefaultCollection}, vbid, nullptr, {});
        EXPECT_EQ(ENGINE_SUCCESS, result.getStatus()) << "Problem with " << key;
        delete result.getValue();
    }

    responseRollback(rollbackPoint);

    // All keys now gone
    for (int ii = 0; ii < items; ii++) {
        std::string key = "anykey_0_" + std::to_string(ii);
        auto result = store->get(
                {key, DocNamespace::DefaultCollection}, vbid, nullptr, {});
        EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus()) << "Problem with "
                                                         << key;
    }

    // Expected a rollback to 0 which is a VB reset, so discard the now dead
    // vb and obtain replacement
    vb = store->getVBucket(vbid);

    // Rollback complete and will have posted a new StreamRequest
    stepForStreamRequest(rollbackPoint,
                         vb->failovers->getLatestEntry().vb_uuid);
    EXPECT_EQ(rollbackPoint, vb->getHighSeqno()) << "VB hasn't rolled back to "
                                                 << rollbackPoint;
}

/**
 * Push stream responses to a consumer and test
 * 1. The first rollback to 0 response is ignored, the consumer requests again
 *    with new data.
 * 2. The second rollback response is non-zero, and the consumer accepts that
 *    and rolls back to the rollbackPoint and requests a stream for it.
 */
TEST_F(RollbackDcpTest, test_rollback_nonzero) {
    const int items = 10;
    const int flushes = 4;
    const int nitems = items * flushes;
    const int rollbackPoint = 3 * items; // rollback to 3/4

    // Test will create anykey_{0..flushes-1}_{0..items-1}
    createItems(items, flushes);

    auto uuid = addStream(nitems);

    responseNoRollback(nitems, 0, uuid);

    // All keys available
    for (int ii = 0; ii < items; ii++) {
        for (int ff = 0; ff < flushes; ff++) {
            std::string key =
                    "anykey_" + std::to_string(ff) + "_" + std::to_string(ii);
            auto result = store->get(
                    {key, DocNamespace::DefaultCollection}, vbid, nullptr, {});
            EXPECT_EQ(ENGINE_SUCCESS, result.getStatus()) << "Expected to find "
                                                          << key;
            delete result.getValue();
        }
    }

    responseRollback(rollbackPoint);

    // 3/4 keys available
    for (int ii = 0; ii < items; ii++) {
        for (int ff = 0; ff < 3; ff++) {
            std::string key =
                    "anykey_" + std::to_string(ff) + "_" + std::to_string(ii);
            auto result = store->get(
                    {key, DocNamespace::DefaultCollection}, vbid, nullptr, {});
            EXPECT_EQ(ENGINE_SUCCESS, result.getStatus()) << "Expected to find "
                                                          << key;
            delete result.getValue();
        }
    }

    // Final 1/4 were discarded by the rollback
    for (int ii = 0; ii < items; ii++) {
        std::string key = "anykey_3_" + std::to_string(ii);
        auto result = store->get(
                {key, DocNamespace::DefaultCollection}, vbid, nullptr, {});
        EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus()) << "Problem with "
                                                         << key;
    }

    // Rollback complete and will have posted a new StreamRequest
    stepForStreamRequest(rollbackPoint,
                         vb->failovers->getLatestEntry().vb_uuid);
    EXPECT_EQ(rollbackPoint, vb->getHighSeqno()) << "VB hasn't rolled back to "
                                                 << rollbackPoint;
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(FullAndValueEviction,
                        RollbackTest,
                        ::testing::Values("value_only", "full_eviction"),
                        [] (const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });
