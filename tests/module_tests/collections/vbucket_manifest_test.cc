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

#include "checkpoint.h"
#include "collections/manifest.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_serialised_manifest_entry.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "tests/module_tests/test_helpers.h"

#include <cJSON_utils.h>

#include <gtest/gtest.h>

class MockVBManifest : public Collections::VB::Manifest {
public:
    MockVBManifest() : Collections::VB::Manifest({/* no collection data*/}) {
    }

    MockVBManifest(const std::string& json) : Collections::VB::Manifest(json) {
    }

    bool exists(const std::string& collection, uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        return exists_UNLOCKED(collection, rev);
    }

    bool isOpen(const std::string& collection, uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isOpen();
    }

    bool isExclusiveOpen(const std::string& collection, uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isExclusiveOpen();
    }

    bool isDeleting(const std::string& collection, uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isDeleting();
    }

    bool isExclusiveDeleting(const std::string& collection,
                             uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isExclusiveDeleting();
    }

    bool isOpenAndDeleting(const std::string& collection, uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isOpenAndDeleting();
    }

    size_t size() const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        return map.size();
    }

    bool compareEntry(const Collections::VB::ManifestEntry& entry) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        if (exists_UNLOCKED(entry.getCollectionName(), entry.getRevision())) {
            auto itr = map.find(entry.getCollectionName());
            const auto& myEntry = *itr->second;
            return myEntry.getStartSeqno() == entry.getStartSeqno() &&
                   myEntry.getEndSeqno() == entry.getEndSeqno() &&
                   myEntry.getRevision() == entry.getRevision();
        }
        return false;
    }

    bool operator==(const MockVBManifest& rhs) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        if (rhs.size() != size()) {
            return false;
        }
        // Check all collections match
        for (const auto& e : map) {
            if (!rhs.compareEntry(*e.second)) {
                return false;
            }
        }

        // finally check the separator's match
        return rhs.separator == separator;
    }

    bool operator!=(const MockVBManifest& rhs) const {
        return !(*this == rhs);
    }

protected:
    bool exists_UNLOCKED(const std::string& collection, uint32_t rev) const {
        auto itr = map.find(collection);
        return itr != map.end() && itr->second->getRevision() == rev;
    }

    void expect_true(bool in) const {
        if (!in) {
            throw std::logic_error("expect_true found false");
        }
    }
};

/**
 * Test class that owns an active and replica manifest.
 * Updates applied to the active are applied to the replica by processing
 * the active's checkpoint.
 */
class ActiveReplicaManifest {
public:
    /// Dummy callback to replace the flusher callback so we can create VBuckets
    class DummyCB : public Callback<uint16_t> {
    public:
        DummyCB() {
        }

        void callback(uint16_t& dummy) {
        }
    };

    ActiveReplicaManifest()
        : active(),
          replica(),
          vbA(0,
              vbucket_state_active,
              global_stats,
              checkpoint_config,
              /*kvshard*/ nullptr,
              /*lastSeqno*/ 0,
              /*lastSnapStart*/ 0,
              /*lastSnapEnd*/ 0,
              /*table*/ nullptr,
              std::make_shared<DummyCB>(),
              /*newSeqnoCb*/ nullptr,
              config,
              VALUE_ONLY),
          vbR(1,
              vbucket_state_replica,
              global_stats,
              checkpoint_config,
              /*kvshard*/ nullptr,
              /*lastSeqno*/ 0,
              /*lastSnapStart*/ 0,
              /*lastSnapEnd*/ snapEnd,
              /*table*/ nullptr,
              std::make_shared<DummyCB>(),
              /*newSeqnoCb*/ nullptr,
              config,
              VALUE_ONLY) {
    }

    ::testing::AssertionResult update(const char* json) {
        try {
            active.wlock().update(vbA, {json});
        } catch (std::exception& e) {
            return ::testing::AssertionFailure()
                   << "Exception thrown for update with " << json
                   << ", e.what:" << e.what();
        }
        queued_item manifest;
        try {
            manifest = applyCheckpointEventsToReplica();
        } catch (std::exception& e) {
            return ::testing::AssertionFailure()
                   << "Exception thrown for replica update, e.what:"
                   << e.what();
        }
        if (active != replica) {
            return ::testing::AssertionFailure()
                   << "active doesn't match replica active:\n"
                   << active << " replica:\n"
                   << replica;
        }
        return checkJson(*manifest);
    }

    ::testing::AssertionResult completeDeletion(const std::string& collection,
                                                uint32_t revision) {
        try {
            active.wlock().completeDeletion(vbA, collection, revision);
            replica.wlock().completeDeletion(vbR, collection, revision);
        } catch (std::exception& e) {
            return ::testing::AssertionFailure()
                   << "Exception thrown for completeDeletion with e.what:"
                   << e.what();
        }

        // completeDeletion adds a new item without a seqno, which closes
        // the snapshot, re-open the snapshot so tests can continue.
        vbR.checkpointManager.updateCurrentSnapshotEnd(snapEnd);
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult doesKeyContainValidCollection(DocKey key) {
        if (!active.lock().doesKeyContainValidCollection(key)) {
            return ::testing::AssertionFailure() << "active failed the key";
        } else if (!replica.lock().doesKeyContainValidCollection(key)) {
            return ::testing::AssertionFailure() << "replica failed the key";
        }
        return ::testing::AssertionSuccess();
    }

    bool isExclusiveOpen(const std::string& collection, uint32_t rev) {
        return active.isExclusiveOpen(collection, rev) &&
               replica.isExclusiveOpen(collection, rev);
    }

    bool isExclusiveDeleting(const std::string& collection, uint32_t rev) {
        return active.isExclusiveDeleting(collection, rev) &&
               replica.isExclusiveDeleting(collection, rev);
    }

    bool isOpenAndDeleting(const std::string& collection, uint32_t rev) {
        return active.isOpenAndDeleting(collection, rev) &&
               replica.isOpenAndDeleting(collection, rev);
    }

    bool checkSize(int s) {
        return active.size() == s && replica.size() == s;
    }

    VBucket& getActiveVB() {
        return vbA;
    }

    MockVBManifest& getActiveManifest() {
        return active;
    }

private:
    static std::string itemToJson(const Item& item) {
        cb::const_char_buffer buffer(item.getData(), item.getNBytes());
        return Collections::VB::Manifest::serialToJson(
                SystemEvent(item.getFlags()), buffer, item.getBySeqno());
    }

    static void getEventsFromCheckpoint(VBucket& vb,
                                        std::vector<queued_item>& events) {
        std::vector<queued_item> items;
        vb.checkpointManager.getAllItemsForCursor(
                CheckpointManager::pCursorName, items);
        for (const auto& qi : items) {
            if (qi->getOperation() == queue_op::system_event) {
                events.push_back(qi);
            }
        }

        if (events.empty()) {
            throw std::logic_error("getEventsFromCheckpoint: no events in vb:" +
                                   std::to_string(vb.getId()));
        }
    }

    /**
     * 1. scan the VBucketManifestTestVBucket's checkpoint for all system
     * events.
     * 2. for all system-events, pretend to be the DcpConsumer and call
     *    the VBucket's manifest's replica functions on.
     * @param replicaVB A vbucket acting as the replica, we will create/delete
     *        collections against this VB.
     * @param replicaManfiest The replica VB's manifest, we will create/delete
     *         collections against this manifest.
     *
     * @returns the last queued_item (which would be used to create a json
     *          manifest)
     */
    queued_item applyCheckpointEventsToReplica() {
        std::vector<queued_item> events;
        getEventsFromCheckpoint(vbA, events);
        queued_item rv = events.back();
        for (const auto& qi : events) {
            if (qi->getOperation() == queue_op::system_event) {
                auto dcpData = Collections::VB::Manifest::getSystemEventData(
                        {qi->getData(),
                         qi->getNBytes()});

                // Extract the revision to a local
                uint32_t revision = *reinterpret_cast<const uint32_t*>(
                        dcpData.second.data());

                switch (SystemEvent(qi->getFlags())) {
                case SystemEvent::CreateCollection: {
                    replica.wlock().replicaAdd(vbR, {dcpData.first.data(), dcpData.first.size()}, revision, qi->getBySeqno());
                    break;
                }
                case SystemEvent::BeginDeleteCollection: {
                    replica.wlock().replicaBeginDelete(vbR, {dcpData.first.data(), dcpData.first.size()}, revision, qi->getBySeqno());
                    break;
                }
                case SystemEvent::CollectionsSeparatorChanged: {
                    auto dcpData = Collections::VB::Manifest::
                            getSystemEventSeparatorData({qi->getData(), qi->getNBytes()});
                    replica.wlock().replicaChangeSeparator(vbR, {dcpData.first.data(), dcpData.first.size()}, revision, qi->getBySeqno());
                    break;
                }
                case SystemEvent::DeleteCollectionSoft:
                case SystemEvent::DeleteCollectionHard:

                    // Nothing todo for these events
                    break;
                }
            }
        }
        return rv;
    }

    /**
     * Take SystemEvent item and obtain the JSON manifest.
     * Next create a new/temp MockVBManifest from the JSON.
     * Finally check that this new object is equal to the test class's active
     *
     * @returns gtest assertion fail (with details) or success
     */
    ::testing::AssertionResult checkJson(const Item& manifest) {
        MockVBManifest newManifest(itemToJson(manifest));
        if (active != newManifest) {
            return ::testing::AssertionFailure() << "manifest mismatch\n"
                                                 << "generated\n"
                                                 << newManifest << "\nvs\n"
                                                 << active;
        }
        return ::testing::AssertionSuccess();
    }

    MockVBManifest active;
    MockVBManifest replica;
    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
    EPVBucket vbA;
    EPVBucket vbR;

    static const int64_t snapEnd{200};
};

class VBucketManifestTest : public ::testing::Test {
public:
    ActiveReplicaManifest manifest;
};

TEST_F(VBucketManifestTest, collectionExists) {
    EXPECT_TRUE(manifest.update(
            R"({"revision":0,"separator":"::","collections":["vegetable"]})"));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));
    EXPECT_TRUE(manifest.isExclusiveOpen("vegetable", 0));
}

TEST_F(VBucketManifestTest, defaultCollectionExists) {
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"anykey", DocNamespace::DefaultCollection}));
    EXPECT_TRUE(manifest.update(
            R"({"revision":1,"separator":"::","collections":[]})"));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"anykey", DocNamespace::DefaultCollection}));
}

TEST_F(VBucketManifestTest, updates) {
    EXPECT_TRUE(manifest.checkSize(1));
    EXPECT_TRUE(manifest.isExclusiveOpen("$default", 0));

    EXPECT_TRUE(manifest.update(
            R"({"revision":1,"separator":"::",)"
            R"("collections":["$default","vegetable"]})"));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveOpen("vegetable", 1));

    EXPECT_TRUE(manifest.update(
            R"({"revision":2,"separator":"::",)"
            R"("collections":["$default", "vegetable", "fruit"]})"));
    EXPECT_TRUE(manifest.checkSize(3));
    EXPECT_TRUE(manifest.isExclusiveOpen("fruit", 2));

    EXPECT_TRUE(manifest.update(
            R"({"revision":3,"separator":"::",)"
            R"("collections":["$default", "vegetable", "fruit", "meat", "dairy"]})"));
    EXPECT_TRUE(manifest.checkSize(5));
    EXPECT_TRUE(manifest.isExclusiveOpen("meat", 3));
    EXPECT_TRUE(manifest.isExclusiveOpen("dairy", 3));
}

TEST_F(VBucketManifestTest, updates2) {
    EXPECT_TRUE(manifest.update(
            R"({"revision":0,"separator":"::",)"
            R"("collections":["$default", "vegetable", "fruit", "meat", "dairy"]})"));
    EXPECT_TRUE(manifest.checkSize(5));

    // Remove meat and dairy, size is not affected because the delete is only
    // starting
    EXPECT_TRUE(manifest.update(
            R"({"revision":1,"separator":"::",)"
            R"("collections":["$default", "vegetable", "fruit"]})"));
    EXPECT_TRUE(manifest.checkSize(5));
    EXPECT_TRUE(manifest.isExclusiveDeleting("meat", 1));
    EXPECT_TRUE(manifest.isExclusiveDeleting("dairy", 1));

    // But vegetable is accessible, the others are locked out
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"anykey", DocNamespace::DefaultCollection}));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"dairy::milk", DocNamespace::Collections}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"meat::chicken", DocNamespace::Collections}));
}

TEST_F(VBucketManifestTest, updates3) {
    EXPECT_TRUE(manifest.update(
            R"({"revision":0,"separator":"::",)"
            R"("collections":["$default", "vegetable", "fruit", "meat", "dairy"]})"));
    EXPECT_TRUE(manifest.checkSize(5));

    // Remove everything
    EXPECT_TRUE(manifest.update(
            R"({"revision":1, "separator":"::","collections":[]})"));
    EXPECT_TRUE(manifest.checkSize(5));
    EXPECT_TRUE(manifest.isExclusiveDeleting("$default", 1));
    EXPECT_TRUE(manifest.isExclusiveDeleting("vegetable", 1));
    EXPECT_TRUE(manifest.isExclusiveDeleting("fruit", 1));
    EXPECT_TRUE(manifest.isExclusiveDeleting("meat", 1));
    EXPECT_TRUE(manifest.isExclusiveDeleting("dairy", 1));

    // But vegetable is accessible, the others are 'locked' out
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"dairy::milk", DocNamespace::Collections}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"meat::chicken", DocNamespace::Collections}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"fruit::apple", DocNamespace::Collections}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"anykey", DocNamespace::DefaultCollection}));
}

TEST_F(VBucketManifestTest, add_beginDelete_add) {
    // add vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":0,"separator":"::","collections":["vegetable"]})"));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveOpen("vegetable", 0));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));

    // remove vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":1,"separator":"::","collections":[]})"));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveDeleting("vegetable", 1));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));
    // add vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":2,"separator":"::","collections":["vegetable"]})"));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isOpenAndDeleting("vegetable", 2));

    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));
}

TEST_F(VBucketManifestTest, add_beginDelete_delete) {
    // add vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":0,"separator":"::","collections":["vegetable"]})"));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveOpen("vegetable", 0));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));

    // remove vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":1,"separator":"::","collections":[]})"));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveDeleting("vegetable", 1));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));

    // finally remove vegetable
    EXPECT_TRUE(manifest.completeDeletion("vegetable", 1));
    EXPECT_TRUE(manifest.checkSize(1));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));
}

TEST_F(VBucketManifestTest, add_beginDelete_add_delete) {
    // add vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":0,"separator":"::","collections":["vegetable"]})"));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveOpen("vegetable", 0));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));

    // remove vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":1,"separator":"::","collections":[]})"));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveDeleting("vegetable", 1));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));

    // add vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":2,"separator":"::","collections":["vegetable"]})"));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isOpenAndDeleting("vegetable", 2));

    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));

    // finally remove vegetable
    EXPECT_TRUE(manifest.completeDeletion("vegetable", 3));
    EXPECT_TRUE(manifest.checkSize(2));

    // No longer OpenAndDeleting, now ExclusiveOpen
    EXPECT_TRUE(manifest.isExclusiveOpen("vegetable", 2));

    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));
}

TEST_F(VBucketManifestTest, invalidDeletes) {
    // add vegetable
    EXPECT_TRUE(manifest.update(R"({"revision":1,"separator":"::",)"
                                R"("collections":["$default","vegetable"]})"));
    // Delete vegetable
    EXPECT_TRUE(manifest.update(R"({"revision":2,"separator":"::",)"
                                R"("collections":["$default"]})"));

    // Invalid.
    EXPECT_FALSE(manifest.completeDeletion("unknown", 1));
    EXPECT_FALSE(manifest.completeDeletion("$default", 1));

    EXPECT_TRUE(manifest.completeDeletion("vegetable", 1));

    // Delete $default
    EXPECT_TRUE(manifest.update(R"({"revision":3,"separator":"::",)"
                                R"("collections":[]})"));
    // Add $default
    EXPECT_TRUE(manifest.update(R"({"revision":4,"separator":"::",)"
                                R"("collections":["$default"]})"));
    EXPECT_TRUE(manifest.completeDeletion("$default", 3));
}

// Check that a deleting collection doesn't keep adding system events
TEST_F(VBucketManifestTest, doubleDelete) {
    auto seqno = manifest.getActiveVB().getHighSeqno();
    // add vegetable
    EXPECT_TRUE(manifest.update(R"({"revision":1,"separator":"::",)"
                                R"("collections":["$default","vegetable"]})"));
    EXPECT_LT(seqno, manifest.getActiveVB().getHighSeqno());
    seqno = manifest.getActiveVB().getHighSeqno();

    // Apply same manifest (different revision). Nothing will be created or
    // deleted. Apply direct to vbm, not via manifest.update as that would
    // complain about the lack of events
    manifest.getActiveManifest().wlock().update(
            manifest.getActiveVB(),
            {R"({"revision":2,"separator":"::",)"
             R"("collections":["$default","vegetable"]})"});

    EXPECT_EQ(seqno, manifest.getActiveVB().getHighSeqno());
    seqno = manifest.getActiveVB().getHighSeqno();

    // Now delete vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":3,"separator":"::","collections":["$default"]})"));

    EXPECT_LT(seqno, manifest.getActiveVB().getHighSeqno());
    seqno = manifest.getActiveVB().getHighSeqno();

    // same again, should have be nothing created or deleted
    manifest.getActiveManifest().wlock().update(
            manifest.getActiveVB(),
            {R"({"revision":4,"separator":"::",)"
             R"("collections":["$default"]})"});

    EXPECT_EQ(seqno, manifest.getActiveVB().getHighSeqno());
}

// This test changes the separator and propagates to the replica (all done
// via the noThrow helper functions).
TEST_F(VBucketManifestTest, active_replica_separatorChanges) {
    // Can change separator to @ as only default exists
    EXPECT_TRUE(manifest.update(
            R"({"revision":1, "separator":"@", "collections":["$default"]})"));

    // Can change separator to / and add first collection
    EXPECT_TRUE(manifest.update(
            R"({"revision":2, "separator":"/", "collections":["$default", "vegetable"]})"));

    // Cannot change separator to ## because non-default collections exist
    EXPECT_FALSE(manifest.update(
            R"({"revision":3, "separator":"##", "collections":["$default", "vegetable"]})"));

    // Now just remove vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":3, "separator":"/", "collections":["$default"]})"));

    // vegetable still exists (isDeleting), but change to ##
    EXPECT_TRUE(manifest.update(
            R"({"revision":4, "separator":"##", "collections":["$default"]})"));

    // Finish removal of vegetable
    EXPECT_TRUE(manifest.completeDeletion("vegetable", 4));

    // Can change separator as only default exists
    EXPECT_TRUE(manifest.update(
            R"({"revision":5, "separator":"@", "collections":["$default"]})"));

    // Remove default
    EXPECT_TRUE(manifest.update(
            R"({"revision":6, "separator":"/", "collections":[]})"));

    // $default still exists (isDeleting), so cannot change to ##
    EXPECT_TRUE(manifest.update(
            R"({"revision":7, "separator":"##", "collections":["$default"]})"));

    EXPECT_TRUE(manifest.completeDeletion("$default", 5));

    // Can change separator as no collection exists
    EXPECT_TRUE(manifest.update(
            R"({"revision":8, "separator":"-=-=-=-", "collections":[]})"));

    // Add a collection and check the new separator
    EXPECT_TRUE(manifest.update(
            R"({"revision":9, "separator":"-=-=-=-", "collections":["meat"]})"));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"meat-=-=-=-bacon", DocNamespace::Collections}));
}

TEST_F(VBucketManifestTest, replica_add_remove) {
    // add vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":1,"separator":"::","collections":)"
            R"(["$default","vegetable"]})"));

    // add meat & dairy
    EXPECT_TRUE(manifest.update(
            R"({"revision":2,"separator":"::","collections":)"
            R"(["$default","vegetable","meat","dairy"]})"));

    // remove vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":3,"separator":"::","collections":)"
            R"(["$default","meat","dairy"]})"));

    // remove $default
    EXPECT_TRUE(manifest.update(
            R"({"revision":4,"separator":"::","collections":)"
            R"(["meat","dairy"]})"));

    // Check we can access the remaining collections
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable::carrot", DocNamespace::Collections}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"anykey", DocNamespace::DefaultCollection}));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"meat::sausage", DocNamespace::Collections}));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"dairy::butter", DocNamespace::Collections}));
}

TEST_F(VBucketManifestTest, replica_add_remove_completeDelete) {
    // add vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":1,"separator":"::","collections":["$default","vegetable"]})"));

    // remove vegetable
    EXPECT_TRUE(manifest.update(
            R"({"revision":2,"separator":"::","collections":["$default"]})"));

    // Finish removal of vegetable
    EXPECT_TRUE(manifest.completeDeletion("vegetable", 2));
}