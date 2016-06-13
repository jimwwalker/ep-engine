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

#include "../mock/mock_dcp_producer.h"
#include "dcp/response.h"
#include "evp_store_single_threaded_test.h"
#include "metaevent.h"

/*
 * Test that the meta event affects how any items are flushed
 */
TEST_F(SingleThreadedEPStoreTest, metaEventBasic) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    engine->getEpStore()->queueMetaEvent(vbid, MetaEvent::CreateCollection);

    const int n = 32;
    for (int i = 0; i < n; i++) {
        std::string key = "key" + std::to_string(i);
        store_item(vbid, key, "value of " + key);
    }

    // Directly flush the vbucket and expect n +  items.
    EXPECT_EQ(n + 1, store->flushVBucket(vbid));
}

/*
 * Test that the meta-events get ignored when streaming DCP.
 * This test will evolve to cover events when we expose them via DCP
 */
TEST_F(SingleThreadedEPStoreTest, metaEventDcp) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    engine->getEpStore()->queueMetaEvent(vbid, MetaEvent::CreateCollection);
    const int n = 32;
    for (int i = 0; i < n; i++) {
        std::string key = "key" + std::to_string(i);
        store_item(vbid, key, "value of " + key);
    }

    // Directly flush the vbucket and expect n +  items.
    EXPECT_EQ(n + 1, store->flushVBucket(vbid));

    {
        // Create a Mock Dcp producer
        SingleThreadedRCPtr<MockDcpProducer> producer = new MockDcpProducer(*engine,
                                                                            cookie,
                                                                            "test",
                                                              /*notifyOnly*/false);

        uint64_t rollbackSeqno;
        // Stream from 0
        EXPECT_EQ(ENGINE_SUCCESS, producer->streamRequest(/*flags*/DCP_ADD_STREAM_FLAG_LATEST|DCP_ADD_STREAM_FLAG_DISKONLY ,
                                                          /*opaque*/0,
                                                          /*vbucket*/vbid,
                                                          /*start_seqno*/0,
                                                          /*end_seqno*/-1,
                                                          /*vb_uuid*/0xabcd,
                                                          /*snap_start*/0,
                                                          /*snap_end*/0,
                                                          &rollbackSeqno,
                                                          SingleThreadedEPStoreTest::fakeDcpAddFailoverLog));

        auto& auxio = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];

        // Run the AUXIO three times. So we get a backfill start, scan and finish.
        runNextTask(auxio,"Backfilling items for a DCP Connection");
        runNextTask(auxio,"Backfilling items for a DCP Connection");
        runNextTask(auxio,"Backfilling items for a DCP Connection");

        std::unique_ptr<DcpResponse> item(producer->public_getNextItem());
        ASSERT_NE(nullptr, item);
        EXPECT_EQ(DCP_SNAPSHOT_MARKER, item->getEvent());

        // n mutations, but no events
        for (int i = 0; i < n; i++) {
            item.reset(producer->public_getNextItem());
            ASSERT_NE(nullptr, item);
            EXPECT_EQ(DCP_MUTATION, item->getEvent());
        }
        item.reset(producer->public_getNextItem());
        EXPECT_EQ(DCP_STREAM_END, item->getEvent());
        // Now get nullptr
        EXPECT_EQ(nullptr, producer->public_getNextItem());
        producer->closeAllStreams();
    }
}