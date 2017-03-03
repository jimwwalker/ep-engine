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

#pragma once

#include "config.h"

#include "bloomfilter.h"
#include "checkpoint.h"
#include "collections/vbucket_manifest.h"
#include "hash_table.h"
#include "hlc.h"
#include "item_pager.h"
#include "monotonic.h"

#include <platform/non_negative_counter.h>
#include <atomic>
#include <queue>

class EPStats;
class ConflictResolution;
class Configuration;
class PreLinkDocumentContext;

/**
 * The following will be used to identify
 * the source of an item's expiration.
 */
enum class ExpireBy { Pager, Compactor, Access };

/* Structure that holds info needed for notification for an item being updated
   in the vbucket */
struct VBNotifyCtx {
    VBNotifyCtx() : bySeqno(0), notifyReplication(false), notifyFlusher(false) {
    }
    Monotonic<int64_t> bySeqno;
    bool notifyReplication;
    bool notifyFlusher;
};

/**
 * Structure that holds info needed to queue an item in chkpt or vb backfill
 * queue
 */
struct VBQueueItemCtx {
    VBQueueItemCtx(GenerateBySeqno genBySeqno,
                   GenerateCas genCas,
                   TrackCasDrift trackCasDrift,
                   bool isBackfillItem,
                   PreLinkDocumentContext* preLinkDocumentContext_)
        : genBySeqno(genBySeqno),
          genCas(genCas),
          trackCasDrift(trackCasDrift),
          isBackfillItem(isBackfillItem),
          preLinkDocumentContext(preLinkDocumentContext_) {
    }
    /* Indicates if we should queue an item or not. If this is false other
       members should not be used */
    GenerateBySeqno genBySeqno;
    GenerateCas genCas;
    TrackCasDrift trackCasDrift;
    bool isBackfillItem;
    PreLinkDocumentContext* preLinkDocumentContext;
};

typedef std::unique_ptr<Callback<const uint16_t, const VBNotifyCtx&>>
        NewSeqnoCallback;

/**
 * Function object that returns true if the given vbucket is acceptable.
 */
class VBucketFilter {
public:

    /**
     * Instiatiate a VBucketFilter that always returns true.
     */
    explicit VBucketFilter() : acceptable() {}

    /**
     * Instantiate a VBucketFilter that returns true for any of the
     * given vbucket IDs.
     */
    explicit VBucketFilter(const std::vector<uint16_t> &a) :
        acceptable(a.begin(), a.end()) {}

    explicit VBucketFilter(const std::set<uint16_t> &s) : acceptable(s) {}

    void assign(const std::set<uint16_t> &a) {
        acceptable = a;
    }

    bool operator ()(uint16_t v) const {
        return acceptable.empty() || acceptable.find(v) != acceptable.end();
    }

    size_t size() const { return acceptable.size(); }

    bool empty() const { return acceptable.empty(); }

    void reset() {
        acceptable.clear();
    }

    /**
     * Calculate the difference between this and another filter.
     * If "this" contains elements, [1,2,3,4] and other contains [3,4,5,6]
     * the returned filter contains: [1,2,5,6]
     * @param other the other filter to compare with
     * @return a new filter with the elements present in only one of the two
     *         filters.
     */
    VBucketFilter filter_diff(const VBucketFilter &other) const;

    /**
     * Calculate the intersection between this and another filter.
     * If "this" contains elements, [1,2,3,4] and other contains [3,4,5,6]
     * the returned filter contains: [3,4]
     * @param other the other filter to compare with
     * @return a new filter with the elements present in both of the two
     *         filters.
     */
    VBucketFilter filter_intersection(const VBucketFilter &other) const;

    const std::set<uint16_t> &getVBSet() const { return acceptable; }

    bool addVBucket(uint16_t vbucket) {
        std::pair<std::set<uint16_t>::iterator, bool> rv = acceptable.insert(vbucket);
        return rv.second;
    }

    void removeVBucket(uint16_t vbucket) {
        acceptable.erase(vbucket);
    }

    /**
     * Dump the filter in a human readable form ( "{ bucket, bucket, bucket }"
     * to the specified output stream.
     */
    friend std::ostream& operator<< (std::ostream& out,
                                     const VBucketFilter &filter);

private:

    std::set<uint16_t> acceptable;
};

class EventuallyPersistentEngine;
class FailoverTable;
class KVShard;

/**
 * An individual vbucket.
 */
class VBucket : public RCValue {
public:

    // Identifier for a vBucket
    typedef uint16_t id_type;

    VBucket(id_type i,
            vbucket_state_t newState,
            EPStats& st,
            CheckpointConfig& chkConfig,
            int64_t lastSeqno,
            uint64_t lastSnapStart,
            uint64_t lastSnapEnd,
            std::unique_ptr<FailoverTable> table,
            std::shared_ptr<Callback<id_type>> flusherCb,
            NewSeqnoCallback newSeqnoCb,
            Configuration& config,
            item_eviction_policy_t evictionPolicy,
            vbucket_state_t initState = vbucket_state_dead,
            uint64_t purgeSeqno = 0,
            uint64_t maxCas = 0);

    virtual ~VBucket();

    int64_t getHighSeqno() const {
        return checkpointManager.getHighSeqno();
    }

    size_t getChkMgrMemUsage() {
        return checkpointManager.getMemoryUsage();
    }

    size_t getChkMgrMemUsageOfUnrefCheckpoints() {
        return checkpointManager.getMemoryUsageOfUnrefCheckpoints();
    }

    uint64_t getPurgeSeqno() const {
        return purge_seqno;
    }

    void setPurgeSeqno(uint64_t to) {
        purge_seqno = to;
    }

    void setPersistedSnapshot(uint64_t start, uint64_t end) {
        LockHolder lh(snapshotMutex);
        persisted_snapshot_start = start;
        persisted_snapshot_end = end;
    }

    snapshot_range_t getPersistedSnapshot() const {
        LockHolder lh(snapshotMutex);
        return {persisted_snapshot_start, persisted_snapshot_end};
    }

    uint64_t getMaxCas() const {
        return hlc.getMaxHLC();
    }

    void setMaxCas(uint64_t cas) {
        hlc.setMaxHLC(cas);
    }

    void setMaxCasAndTrackDrift(uint64_t cas) {
        hlc.setMaxHLCAndTrackDrift(cas);
    }

    void forceMaxCas(uint64_t cas) {
        hlc.forceMaxHLC(cas);
    }

    HLC::DriftStats getHLCDriftStats() const {
        return hlc.getDriftStats();
    }

    HLC::DriftExceptions getHLCDriftExceptionCounters() const {
        return hlc.getDriftExceptionCounters();
    }

    void setHLCDriftAheadThreshold(std::chrono::microseconds threshold) {
        hlc.setDriftAheadThreshold(threshold);
    }

    void setHLCDriftBehindThreshold(std::chrono::microseconds threshold) {
        hlc.setDriftBehindThreshold(threshold);
    }

    bool isTakeoverBackedUp() {
        return takeover_backed_up.load();
    }

    void setTakeoverBackedUpState(bool to) {
        bool inverse = !to;
        takeover_backed_up.compare_exchange_strong(inverse, to);
    }

    // States whether the VBucket is in the process of being created
    bool isBucketCreation() const {
        return bucketCreation.load();
    }

    bool setBucketCreation(bool rv) {
        bool inverse = !rv;
        return bucketCreation.compare_exchange_strong(inverse, rv);
    }

    // States whether the VBucket is in the process of being deleted
    bool isBucketDeletion() const {
        return bucketDeletion.load();
    }

    bool setBucketDeletion(bool delBucket) {
        bool inverse = !delBucket;
        return bucketDeletion.compare_exchange_strong(inverse, delBucket);
    }

    // Returns the last persisted sequence number for the VBucket
    uint64_t getPersistenceSeqno() const {
        return persistenceSeqno.load();
    }

    void setPersistenceSeqno(uint64_t seqno) {
        persistenceSeqno.store(seqno);
    }

    id_type getId() const { return id; }
    vbucket_state_t getState(void) const { return state.load(); }
    void setState(vbucket_state_t to);
    cb::RWLock& getStateLock() {return stateLock;}

    vbucket_state_t getInitialState(void) { return initialState; }
    void setInitialState(vbucket_state_t initState) {
        initialState = initState;
    }

    vbucket_state getVBucketState() const;

    /**
     * This method performs operations on the stored value prior
     * to expiring the item.
     *
     * @param v the stored value
     */
    void handlePreExpiry(StoredValue& v);

    bool addPendingOp(const void *cookie) {
        LockHolder lh(pendingOpLock);
        if (state != vbucket_state_pending) {
            // State transitioned while we were waiting.
            return false;
        }
        // Start a timer when enqueuing the first client.
        if (pendingOps.empty()) {
            pendingOpsStart = gethrtime();
        }
        pendingOps.push_back(cookie);
        ++stats.pendingOps;
        ++stats.pendingOpsTotal;
        return true;
    }

    void doStatsForQueueing(const Item& item, size_t itemBytes);
    void doStatsForFlushing(const Item& item, size_t itemBytes);
    void incrMetaDataDisk(const Item& qi);
    void decrMetaDataDisk(const Item& qi);

    void resetStats();

    // Get age sum in millisecond
    uint64_t getQueueAge() {
        uint64_t currDirtyQueueAge = dirtyQueueAge.load(
                                                    std::memory_order_relaxed);
        rel_time_t currentAge = ep_current_time() * dirtyQueueSize;
        if (currentAge < currDirtyQueueAge) {
            return 0;
        }
        return (currentAge - currDirtyQueueAge) * 1000;
    }

    void fireAllOps(EventuallyPersistentEngine &engine);

    size_t size(void) {
        HashTableDepthStatVisitor v;
        ht.visitDepth(v);
        return v.size;
    }

    size_t getBackfillSize() {
        LockHolder lh(backfill.mutex);
        return backfill.items.size();
    }
    bool queueBackfillItem(queued_item& qi,
                           const GenerateBySeqno generateBySeqno) {
        LockHolder lh(backfill.mutex);
        if (GenerateBySeqno::Yes == generateBySeqno) {
            qi->setBySeqno(checkpointManager.nextBySeqno());
        } else {
            checkpointManager.setBySeqno(qi->getBySeqno());
        }
        backfill.items.push(qi);
        ++stats.diskQueueSize;
        ++stats.totalEnqueued;
        doStatsForQueueing(*qi, qi->size());
        stats.memOverhead->fetch_add(sizeof(queued_item));
        return true;
    }
    void getBackfillItems(std::vector<queued_item> &items) {
        LockHolder lh(backfill.mutex);
        size_t num_items = backfill.items.size();
        while (!backfill.items.empty()) {
            items.push_back(backfill.items.front());
            backfill.items.pop();
        }
        stats.memOverhead->fetch_sub(num_items * sizeof(queued_item));
    }
    bool isBackfillPhase() {
        LockHolder lh(backfill.mutex);
        return backfill.isBackfillPhase;
    }
    void setBackfillPhase(bool backfillPhase) {
        LockHolder lh(backfill.mutex);
        backfill.isBackfillPhase = backfillPhase;
    }

    /**
     * Returns the map of bgfetch items for this vbucket, clearing the
     * pendingBGFetches.
     */
    virtual vb_bgfetch_queue_t getBGFetchItems() = 0;

    virtual bool hasPendingBGFetchItems() = 0;

    static const char* toString(vbucket_state_t s) {
        switch(s) {
        case vbucket_state_active: return "active"; break;
        case vbucket_state_replica: return "replica"; break;
        case vbucket_state_pending: return "pending"; break;
        case vbucket_state_dead: return "dead"; break;
        }
        return "unknown";
    }

    static vbucket_state_t fromString(const char* state) {
        if (strcmp(state, "active") == 0) {
            return vbucket_state_active;
        } else if (strcmp(state, "replica") == 0) {
            return vbucket_state_replica;
        } else if (strcmp(state, "pending") == 0) {
            return vbucket_state_pending;
        } else {
            return vbucket_state_dead;
        }
    }

    virtual ENGINE_ERROR_CODE addHighPriorityVBEntry(uint64_t id,
                                                     const void* cookie,
                                                     bool isBySeqno) = 0;
    virtual void notifyOnPersistence(EventuallyPersistentEngine& e,
                                     uint64_t id,
                                     bool isBySeqno) = 0;
    virtual void notifyAllPendingConnsFailed(EventuallyPersistentEngine& e) = 0;
    virtual size_t getHighPriorityChkSize() = 0;

    /**
     * BloomFilter operations for vbucket
     */
    void createFilter(size_t key_count, double probability);
    void initTempFilter(size_t key_count, double probability);
    void addToFilter(const DocKey& key);
    virtual bool maybeKeyExistsInFilter(const DocKey& key);
    bool isTempFilterAvailable();
    void addToTempFilter(const DocKey& key);
    void swapFilter();
    void clearFilter();
    void setFilterStatus(bfilter_status_t to);
    std::string getFilterStatusString();
    size_t getFilterSize();
    size_t getNumOfKeysInFilter();

    uint64_t nextHLCCas() {
        return hlc.nextHLC();
    }

    // Applicable only for FULL EVICTION POLICY
    bool isResidentRatioUnderThreshold(float threshold,
                                       item_eviction_policy_t policy);

    virtual void addStats(bool details, ADD_STAT add_stat, const void* c) = 0;

    virtual KVShard* getShard() = 0;

    virtual size_t getNumItems() const = 0;

    size_t getNumNonResidentItems(item_eviction_policy_t policy);

    size_t getNumTempItems(void) {
        return ht.getNumTempItems();
    }

    void incrRollbackItemCount(uint64_t val) {
        rollbackItemCount.fetch_add(val, std::memory_order_relaxed);
    }

    uint64_t getRollbackItemCount(void) {
        return rollbackItemCount.load(std::memory_order_relaxed);
    }

    // Return the persistence checkpoint ID
    uint64_t getPersistenceCheckpointId() const;

    // Set the persistence checkpoint ID to the given value.
    void setPersistenceCheckpointId(uint64_t checkpointId);

    // Mark the value associated with the given key as dirty
    void markDirty(const DocKey& key);

    /**
     * Obtain the read handle for the collections manifest.
     * The caller will have read-only access to manifest using the methods
     * exposed by the ReadHandle
     */
    Collections::VB::Manifest::ReadHandle lockCollections() const {
        return manifest.lock();
    }

    /**
     * Update the Collections::VB::Manifest and the VBucket.
     * Adds SystemEvents for the create and delete of collections into the
     * checkpoint.
     *
     * @param m A Collections::Manifest to apply to the VB::Manifest
     */
    void updateFromManifest(const Collections::Manifest& m) {
        manifest.wlock().update(*this, m);
    }

    /**
     * Finalise the deletion of a collection (no items remain in the collection)
     *
     * @param collection "string-view" name of the collection
     * @param revision The Manifest revision which initiated the delete.
     */
    void completeDeletion(cb::const_char_buffer collection, uint32_t revision) {
        manifest.wlock().completeDeletion(*this, collection, revision);
    }

    static const vbucket_state_t ACTIVE;
    static const vbucket_state_t REPLICA;
    static const vbucket_state_t PENDING;
    static const vbucket_state_t DEAD;

    HashTable         ht;
    CheckpointManager checkpointManager;

    // Struct for managing 'backfill' items - Items which have been added by
    // an incoming TAP stream and need to be persisted to disk.
    struct {
        std::mutex mutex;
        std::queue<queued_item> items;
        bool isBackfillPhase;
    } backfill;

    /**
     * Gets the valid StoredValue for the key and deletes an expired item if
     * desired by the caller. Requires the hash bucket to be locked
     *
     * @param hbl Reference to the hash bucket lock
     * @param key
     * @param wantsDeleted
     * @param trackReference
     * @param queueExpired Delete an expired item
     */
    StoredValue* fetchValidValue(HashTable::HashBucketLock& hbl,
                                 const DocKey& key,
                                 WantsDeleted wantsDeleted,
                                 TrackReference trackReference,
                                 QueueExpired queueExpired);

    /**
     * Complete the background fetch for the specified item. Depending on the
     * state of the item, restore it to the hashtable as appropriate,
     * potentially queuing it as dirty.
     *
     * @param key The key of the item
     * @param fetched_item The item which has been fetched.
     * @param startTime The time processing of the batch of items started.
     *
     * @return ENGINE_ERROR_CODE status notified to be to the front end
     */
    virtual ENGINE_ERROR_CODE completeBGFetchForSingleItem(
            const DocKey& key,
            const VBucketBGFetchItem& fetched_item,
            const ProcessClock::time_point startTime) = 0;

    /**
     * Retrieve an item from the disk for vkey stats
     *
     * @param key the key to fetch
     * @param cookie the connection cookie
     * @param eviction_policy The eviction policy
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     *
     * @return VBReturnCtx indicates notifyCtx and operation result
     */
    virtual ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                        const void* cookie,
                                        EventuallyPersistentEngine& engine,
                                        int bgFetchDelay) = 0;

    /**
     * Complete the vkey stats for an item background fetched from disk.
     *
     * @param key The key of the item
     * @param gcb Bgfetch cbk obj containing the item from disk
     *
     */
    virtual void completeStatsVKey(
            const DocKey& key, const RememberingCallback<GetValue>& gcb) = 0;

    /**
     * Set (add new or update) an item into in-memory structure like
     * hash table and do not generate a seqno. This is called internally from
     * ep-engine when we want to update our in-memory data (like in HT) with
     * another source of truth like disk.
     * Currently called during rollback.
     *
     * @param itm Item to be added or updated. Upon success, the itm
     *            revSeqno are updated
     *
     * @return Result indicating the status of the operation
     */
    MutationStatus setFromInternal(Item& itm);

    /**
     * Set (add new or update) an item in the vbucket.
     *
     * @param itm Item to be added or updated. Upon success, the itm
     *            bySeqno, cas and revSeqno are updated
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     *
     * @return ENGINE_ERROR_CODE status notified to be to the front end
     */
    ENGINE_ERROR_CODE set(Item& itm,
                          const void* cookie,
                          EventuallyPersistentEngine& engine,
                          int bgFetchDelay);

    /**
     * Replace (overwrite existing) an item in the vbucket.
     *
     * @param itm Item to be added or updated. Upon success, the itm
     *            bySeqno, cas and revSeqno are updated
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     *
     * @return ENGINE_ERROR_CODE status notified to be to the front end
     */
    ENGINE_ERROR_CODE replace(Item& itm,
                              const void* cookie,
                              EventuallyPersistentEngine& engine,
                              int bgFetchDelay);

    /**
     * Add an item directly into its vbucket rather than putting it on a
     * checkpoint (backfill the item). The can happen during TAP or when a
     * replica vbucket is receiving backfill items from active vbucket.
     *
     * @param itm Item to be added/updated from TAP or DCP backfill. Upon
     *            success, the itm revSeqno is updated
     * @param genBySeqno whether or not to generate sequence number
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE addBackfillItem(Item& itm, GenerateBySeqno genBySeqno);

    /**
     * Set an item in the store from a non-front end operation (DCP, XDCR)
     *
     * @param item the item to set. Upon success, the itm revSeqno is updated
     * @param cas value to match
     * @param seqno sequence number of mutation
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param force override vbucket states
     * @param allowExisting set to false if you want set to fail if the
     *                      item exists already
     * @param genBySeqno whether or not to generate sequence number
     * @param genCas
     * @param isReplication set to true if we are to use replication
     *                      throttle threshold
     *
     * @return the result of the store operation
     */
    ENGINE_ERROR_CODE setWithMeta(Item& itm,
                                  uint64_t cas,
                                  uint64_t* seqno,
                                  const void* cookie,
                                  EventuallyPersistentEngine& engine,
                                  int bgFetchDelay,
                                  bool force,
                                  bool allowExisting,
                                  GenerateBySeqno genBySeqno,
                                  GenerateCas genCas,
                                  bool isReplication);

    /**
     * Delete an item in the vbucket
     *
     * @param key key to be deleted
     * @param[in,out] cas value to match; new cas after logical delete
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param itm item pointer that contains a value that needs to be
     *            stored along with a delete. A NULL pointer indicates
     *            that no value needs to be stored with the delete.
     * @param[out] itemMeta pointer to item meta data that needs to be returned
     *                      as a result the delete. A NULL pointer indicates
     *                      that no meta data needs to be returned.
     * @param[out] mutInfo Info to uniquely identify (and order) the delete
     *                     seq. A NULL pointer indicates no info needs to be
     *                     returned.
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE deleteItem(const DocKey& key,
                                 uint64_t& cas,
                                 const void* cookie,
                                 EventuallyPersistentEngine& engine,
                                 int bgFetchDelay,
                                 Item* itm,
                                 ItemMetaData* itemMeta,
                                 mutation_descr_t* mutInfo);

    /**
     * Delete an item in the vbucket from a non-front end operation (DCP, XDCR)
     *
     * @param key key to be deleted
     * @param[in, out] cas value to match; new cas after logical delete
     * @param[out] seqno Pointer to get the seqno generated for the item. A
     *                   NULL value is passed if not needed
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param force force a delete in full eviction mode without doing a
     *              bg fetch
     * @param itemMeta ref to item meta data
     * @param backfill indicates if the item must be put onto vb queue or
     *                 onto checkpoint
     * @param genBySeqno whether or not to generate sequence number
     * @param generateCas whether or not to generate cas
     * @param bySeqno seqno of the key being deleted
     * @param isReplication set to true if we are to use replication
     *                      throttle threshold
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE deleteWithMeta(const DocKey& key,
                                     uint64_t& cas,
                                     uint64_t* seqno,
                                     const void* cookie,
                                     EventuallyPersistentEngine& engine,
                                     int bgFetchDelay,
                                     bool force,
                                     const ItemMetaData& itemMeta,
                                     bool backfill,
                                     GenerateBySeqno genBySeqno,
                                     GenerateCas generateCas,
                                     uint64_t bySeqno,
                                     bool isReplication);

    /**
     * Delete an expired item
     *
     * @param key key to be deleted
     * @param startTime the time to be compared with this item's expiry time
     * @param revSeqno revision id sequence number
     * @param source Expiry source
     */
    void deleteExpiredItem(const DocKey& key,
                           time_t startTime,
                           uint64_t revSeqno,
                           ExpireBy source);

    /**
     * Evict a key from memory.
     *
     * @param key Key to evict
     * @param[out] msg Updated to point to a string (with static duration)
     *                 describing the result of the operation.
     *
     * @return SUCCESS if key was successfully evicted (or was already
     *                 evicted), or the reason why the request failed.
     *
     */
    virtual protocol_binary_response_status evictKey(const DocKey& key,
                                                     const char** msg) = 0;

    /**
     * Eject a StoredValue from memory.
     *
     * @param v[in, out] Ref to the StoredValue to be ejected. Based on the
     *                   VBucket type, policy in the vbucket contents of v and
     *                   v itself may be changed
     *
     * @return true if an item is ejected.
     */
    virtual bool htUnlockedEjectItem(StoredValue*& v) = 0;

    /**
     * Add an item in the store
     *
     * @param itm the item to add. On success, this will have its seqno and
     *            CAS updated.
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE add(Item& itm,
                          const void* cookie,
                          EventuallyPersistentEngine& engine,
                          int bgFetchDelay);

    /**
     * Retrieve a value, but update its TTL first
     *
     * @param key the key to fetch
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param exptime the new expiry time for the object
     *
     * @return a GetValue representing the result of the request
     */
    GetValue getAndUpdateTtl(const DocKey& key,
                             const void* cookie,
                             EventuallyPersistentEngine& engine,
                             int bgFetchDelay,
                             time_t exptime);
    /**
     * Queue an Item to the checkpoint and return its seqno
     *
     * @param an Item object to queue, can be any kind of item and will be given
     *        a CAS and seqno by this function.
     */
    int64_t queueItem(Item* item);

    /**
     * Insert an item into the VBucket during warmup. If we're trying to insert
     * a partial item we mark it as nonResident
     *
     * @param itm Item to insert. itm is not modified. But cannot be passed as
     *            const because it is passed to functions that can generally
     *            modify the itm but do not modify it due to the flags passed.
     * @param eject true if we should eject the value immediately
     * @param keyMetaDataOnly is this just the key and meta-data or a complete
     *                        item
     *
     * @return the result of the operation
     */
    MutationStatus insertFromWarmup(Item& itm,
                                    bool eject,
                                    bool keyMetaDataOnly);

    /**
     * Get metadata and value for a given key
     *
     * @param key key for which metadata and value should be retrieved
     * @param cookie the cookie representing the client
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param options flags indicating some retrieval related info
     * @param diskFlushAll
     *
     * @return the result of the operation
     */
    GetValue getInternal(const DocKey& key,
                         const void* cookie,
                         EventuallyPersistentEngine& engine,
                         int bgFetchDelay,
                         get_options_t options,
                         bool diskFlushAll);

    /**
     * Retrieve the meta data for given key
     *
     * @param key the key to get the meta data for
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay Delay in secs before we run the bgFetch task
     * @param[out] metadata meta information returned to the caller
     * @param[out] deleted specifies the caller whether or not the key is
     *                     deleted
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE getMetaData(const DocKey& key,
                                  const void* cookie,
                                  EventuallyPersistentEngine& engine,
                                  int bgFetchDelay,
                                  ItemMetaData& metadata,
                                  uint32_t& deleted);

    /**
     * Looks up the key stats for the given {vbucket, key}.
     *
     * @param key The key to lookup
     * @param cookie The client's cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param[out] kstats On success the keystats for this item.
     * @param wantsDeleted If yes then return keystats even if the item is
     *                     marked as deleted. If no then will return
     *                     ENGINE_KEY_ENOENT for deleted items.
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE getKeyStats(const DocKey& key,
                                  const void* cookie,
                                  EventuallyPersistentEngine& engine,
                                  int bgFetchDelay,
                                  struct key_stats& kstats,
                                  WantsDeleted wantsDeleted);

    /**
     * Gets a locked item for a given key.
     *
     * @param key The key to lookup
     * @param currentTime Current time to use for locking the item for a
     *                    duration of lockTimeout
     * @param lockTimeout Timeout for the lock on the item
     * @param cookie The client's cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay Delay in secs before we run the bgFetch task
     *
     * @return the result of the operation (contains locked item on success)
     */
    GetValue getLocked(const DocKey& key,
                       rel_time_t currentTime,
                       uint32_t lockTimeout,
                       const void* cookie,
                       EventuallyPersistentEngine& engine,
                       int bgFetchDelay);
    /**
     * Update in memory data structures after an item is deleted on disk
     *
     * @param queuedItem reference to the deleted item
     * @param deleted indicates if item actaully deleted or not (in case item
     *                did not exist on disk)
     */
    void deletedOnDiskCbk(const Item& queuedItem, bool deleted);

    /**
     * Update in memory data structures after a rollback on disk
     *
     * @param queuedItem item key
     *
     * @return indicates if the operation is succcessful
     */
    bool deleteKey(const DocKey& key);

    std::queue<queued_item> rejectQueue;
    std::unique_ptr<FailoverTable> failovers;

    std::atomic<size_t>  opsCreate;
    std::atomic<size_t>  opsUpdate;
    std::atomic<size_t>  opsDelete;
    std::atomic<size_t>  opsReject;

    cb::NonNegativeCounter<size_t> dirtyQueueSize;
    std::atomic<size_t>  dirtyQueueMem;
    std::atomic<size_t>  dirtyQueueFill;
    std::atomic<size_t>  dirtyQueueDrain;
    std::atomic<uint64_t> dirtyQueueAge;
    std::atomic<size_t>  dirtyQueuePendingWrites;
    std::atomic<size_t>  metaDataDisk;

    std::atomic<size_t>  numExpiredItems;

protected:
    /**
     * This function checks cas, expiry and other partition (vbucket) related
     * rules before setting an item into other in-memory structure like HT,
     * and checkpoint mgr. This function assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param v Reference to the ptr of StoredValue. This can be changed if a
     *          new StoredValue is added or just its contents is changed if the
     *          exisiting StoredValue is updated
     * @param itm Item to be added/updated. On success, its revSeqno is updated
     * @param cas value to match
     * @param allowExisting set to false if you want set to fail if the
     *                      item exists already
     * @param hasMetaData
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue; NULL if item need not be queued
     * @param maybeKeyExists true if bloom filter predicts that key may exist
     * @param isReplication true if issued by consumer (for replication)
     *
     * @return Result indicating the status of the operation and notification
     *                info
     */
    std::pair<MutationStatus, VBNotifyCtx> processSet(
            const HashTable::HashBucketLock& hbl,
            StoredValue*& v,
            Item& itm,
            uint64_t cas,
            bool allowExisting,
            bool hasMetaData,
            const VBQueueItemCtx* queueItmCtx = nullptr,
            bool maybeKeyExists = true,
            bool isReplication = false);

    /**
     * This function checks cas, expiry and other partition (vbucket) related
     * rules before adding an item into other in-memory structure like HT,
     * and checkpoint mgr. This function assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param v[in, out] the stored value to do this operation on
     * @param itm Item to be added/updated. On success, its revSeqno is updated
     * @param isReplication true if issued by consumer (for replication)
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue; NULL if item need not be queued
     *
     * @return Result indicating the status of the operation and notification
     *                info
     */
    std::pair<AddStatus, VBNotifyCtx> processAdd(
            const HashTable::HashBucketLock& hbl,
            StoredValue*& v,
            Item& itm,
            bool maybeKeyExists,
            bool isReplication,
            const VBQueueItemCtx* queueItmCtx = nullptr);

    /**
     * This function checks cas, eviction policy and other partition
     * (vbucket) related rules before logically (soft) deleting an item in
     * in-memory structure like HT, and checkpoint mgr.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param htLock Hash table lock that must be held
     * @param v Reference to the StoredValue to be soft deleted
     * @param cas the expected CAS of the item (or 0 to override)
     * @param metadata ref to item meta data
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue
     * @param use_meta Indicates if v must be updated with the metadata
     * @param bySeqno seqno of the key being deleted
     *
     * @return Result indicating the status of the operation and notification
     *                info
     */
    std::pair<MutationStatus, VBNotifyCtx> processSoftDelete(
            const std::unique_lock<std::mutex>& htLock,
            StoredValue& v,
            uint64_t cas,
            const ItemMetaData& metadata,
            const VBQueueItemCtx& queueItmCtx,
            bool use_meta,
            uint64_t bySeqno);

    /**
     * Delete a key (associated StoredValue) from ALL in-memory data structures
     * like HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * Currently StoredValues form HashTable intrusively. That is, HashTable
     * does not store a reference or a copy of the StoredValue. If any other
     * in-memory data strucutures are formed intrusively using StoredValues,
     * then it must be decided in this function which data structure deletes
     * the StoredValue. Currently it is HashTable that deleted the StoredValue
     *
     * @param hbl Hash table bucket lock that must be held
     * @param v Reference to the StoredValue to be deleted
     *
     * @return true if an object was deleted, false otherwise
     */
    bool deleteStoredValue(const HashTable::HashBucketLock& hbl,
                           StoredValue& v);

    /**
     * Queue an item for persistence and replication. Maybe track CAS drift
     *
     * The caller of this function must hold the lock of the hash table
     * partition that contains the StoredValue being Queued.
     *
     * @param v the dirty item. The cas and seqno maybe updated based on the
     *          flags passed
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue, whether to track cas, generate seqno,
     *                    generate new cas
     *
     * @return Notification context containing info needed to notify the
     *         clients (like connections, flusher)
     */
    VBNotifyCtx queueDirty(StoredValue& v, const VBQueueItemCtx& queueItmCtx);

    /**
     * Queue an item for persistence and replication
     *
     * The caller of this function must hold the lock of the hash table
     * partition that contains the StoredValue being Queued.
     *
     * @param v the dirty item. The cas and seqno maybe updated based on the
     *          flags passed
     * @param generateBySeqno request that the seqno is generated by this call
     * @param generateCas request that the CAS is generated by this call
     * @param isBackfillItem indicates if the item must be put onto vb queue or
     *        onto checkpoint
     * @param preLinkDocumentContext context object which allows running the
     *        document pre link callback after the cas is assinged (but
     *        but document not available for anyone)
     *
     * @return Notification context containing info needed to notify the
     *         clients (like connections, flusher)
     */
    VBNotifyCtx queueDirty(
            StoredValue& v,
            GenerateBySeqno generateBySeqno = GenerateBySeqno::Yes,
            GenerateCas generateCas = GenerateCas::Yes,
            bool isBackfillItem = false,
            PreLinkDocumentContext* preLinkDocumentContext = nullptr);

    /**
     * Adds a temporary StoredValue in in-memory data structures like HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param key the key for which a temporary item needs to be added
     * @param isReplication true if issued by consumer (for replication)
     *
     * @return Result indicating the status of the operation
     */
    AddStatus addTempStoredValue(const HashTable::HashBucketLock& hbl,
                                 const DocKey& key,
                                 bool isReplication = false);

    void _addStats(bool details, ADD_STAT add_stat, const void* c);

    template <typename T>
    void addStat(const char* nm,
                 const T& val,
                 ADD_STAT add_stat,
                 const void* c);

    /* This member holds the eviction policy used */
    const item_eviction_policy_t eviction;

    /* Reference to global (EP engine wide) stats */
    EPStats& stats;

private:
    void fireAllOps(EventuallyPersistentEngine& engine, ENGINE_ERROR_CODE code);

    void decrDirtyQueueMem(size_t decrementBy);

    void decrDirtyQueueAge(uint32_t decrementBy);

    void decrDirtyQueuePendingWrites(size_t decrementBy);

    /**
     * Updates an existing StoredValue in in-memory data structures like HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param htLock Hash table lock that must be held
     * @param v Reference to the StoredValue to be updated.
     * @param itm Item to be updated.
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue; NULL if item need not be queued
     *
     * @return Result indicating the status of the operation and notification
     *                info
     */
    virtual std::pair<MutationStatus, VBNotifyCtx> updateStoredValue(
            const std::unique_lock<std::mutex>& htLock,
            StoredValue& v,
            const Item& itm,
            const VBQueueItemCtx* queueItmCtx) = 0;

    /**
     * Adds a new StoredValue in in-memory data structures like HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param itm Item to be added.
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue; NULL if item need not be queued
     *
     * @return Ptr of the StoredValue added and notification info
     */
    virtual std::pair<StoredValue*, VBNotifyCtx> addNewStoredValue(
            const HashTable::HashBucketLock& hbl,
            const Item& itm,
            const VBQueueItemCtx* queueItmCtx) = 0;

    /**
     * Logically (soft) delete item in all in-memory data structures. Also
     * updates revSeqno. Depending on the in-memory data structure the item may
     * be marked delete and/or reset and/or a new value (marked as deleted)
     * added.
     * Assumes that HT bucket lock is grabbed.
     * Also assumes that v is in the hash table.
     *
     * @param htLock Hash table lock that must be held
     * @param v Reference to the StoredValue to be soft deleted
     * @param onlyMarkDeleted indicates if we must reset the StoredValue or
     *                        just mark deleted
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue
     * @param bySeqno seqno of the key being deleted
     *
     * @return Notification info
     */
    virtual VBNotifyCtx softDeleteStoredValue(
            const std::unique_lock<std::mutex>& htLock,
            StoredValue& v,
            bool onlyMarkDeleted,
            const VBQueueItemCtx& queueItmCtx,
            uint64_t bySeqno) = 0;

    /**
     * This function handles expiry relatead stuff before logically (soft)
     * deleting an item in in-memory structures like HT, and checkpoint mgr.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param htLock Hash table lock that must be held
     * @param v Reference to the StoredValue to be soft deleted
     *
     * @return Result indicating the status of the operation and notification
     *                info
     */
    std::pair<MutationStatus, VBNotifyCtx> processExpiredItem(
            const std::unique_lock<std::mutex>& htLock, StoredValue& v);

    /**
     * Add a temporary item in hash table and enqueue a background fetch for a
     * key.
     *
     * @param hbl Reference to the hash table bucket lock
     * @param key the key to be bg fetched
     * @param cookie the cookie of the requestor
     * @param engine Reference to ep engine
     * @param bgFetchDelay Delay in secs before we run the bgFetch task
     * @param metadataOnly whether the fetch is for a non-resident value or
     *                     metadata of a (possibly) deleted item
     * @param isReplication indicates if the call is for a replica vbucket
     *
     * @return ENGINE_ERROR_CODE status notified to be to the front end
     */
    virtual ENGINE_ERROR_CODE addTempItemAndBGFetch(
            HashTable::HashBucketLock& hbl,
            const DocKey& key,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            bool metadataOnly,
            bool isReplication = false) = 0;

    /**
     * Enqueue a background fetch for a key.
     *
     * @param key the key to be bg fetched
     * @param cookie the cookie of the requestor
     * @param engine Reference to ep engine
     * @param bgFetchDelay Delay in secs before we run the bgFetch task
     * @param isMeta whether the fetch is for a non-resident value or metadata
     *               of a (possibly) deleted item
     */
    virtual void bgFetch(const DocKey& key,
                         const void* cookie,
                         EventuallyPersistentEngine& engine,
                         int bgFetchDelay,
                         bool isMeta = false) = 0;

    /**
     * Get metadata and value for a non-resident key
     *
     * @param key key for which metadata and value should be retrieved
     * @param cookie the cookie representing the client
     * @param engine Reference to ep engine
     * @param bgFetchDelay Delay in secs before we run the bgFetch task
     * @param options flags indicating some retrieval related info
     * @param v reference to the stored value of the non-resident key
     *
     * @return the result of the operation
     */
    virtual GetValue getInternalNonResident(const DocKey& key,
                                            const void* cookie,
                                            EventuallyPersistentEngine& engine,
                                            int bgFetchDelay,
                                            get_options_t options,
                                            const StoredValue& v) = 0;

    /**
     * Internal wrapper function around the callback to be called when a new
     * seqno is generated in the vbucket
     *
     * @param notifyCtx holds info needed for notification
     */
    void notifyNewSeqno(const VBNotifyCtx& notifyCtx);

    /**
     * Update the revision seqno of a newly StoredValue item.
     * We must ensure that it is greater the maxDeletedRevSeqno
     *
     * @param v StoredValue added newly. Its revSeqno is updated
     */
    void updateRevSeqNoOfNewStoredValue(StoredValue& v);

    /**
     * Increase the expiration count global stats and in the vbucket stats
     */
    void incExpirationStat(ExpireBy source);

    id_type                         id;
    std::atomic<vbucket_state_t>    state;
    cb::RWLock                      stateLock;
    vbucket_state_t                 initialState;
    std::mutex                           pendingOpLock;
    std::vector<const void*>        pendingOps;
    hrtime_t                        pendingOpsStart;
    uint64_t                        purge_seqno;
    std::atomic<bool>               takeover_backed_up;

    /* snapshotMutex is used to update/read the pair {start, end} atomically,
       but not if reading a single field. */
    mutable std::mutex snapshotMutex;
    uint64_t persisted_snapshot_start;
    uint64_t persisted_snapshot_end;

    std::mutex bfMutex;
    std::unique_ptr<BloomFilter> bFilter;
    std::unique_ptr<BloomFilter> tempFilter;    // Used during compaction.

    std::atomic<uint64_t> rollbackItemCount;

    HLC hlc;
    std::string statPrefix;
    // The persistence checkpoint ID for this vbucket.
    std::atomic<uint64_t> persistenceCheckpointId;
    // Flag to indicate the bucket is being created
    std::atomic<bool> bucketCreation;
    // Flag to indicate the bucket is being deleted
    std::atomic<bool> bucketDeletion;
    std::atomic<uint64_t> persistenceSeqno;

    // Ptr to the item conflict resolution module
    std::unique_ptr<ConflictResolution> conflictResolver;

    // A callback to be called when a new seqno is generated in the vbucket as
    // a result of a front end call
    NewSeqnoCallback newSeqnoCb;

    /// The VBucket collection state
    Collections::VB::Manifest manifest;

    DISALLOW_COPY_AND_ASSIGN(VBucket);
};
