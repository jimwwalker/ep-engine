/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "config.h"

#include <limits>
#include <string>

#include "stored-value.h"

#ifndef DEFAULT_HT_SIZE
#define DEFAULT_HT_SIZE 1531
#endif

size_t HashTableStorage::defaultNumBuckets = DEFAULT_HT_SIZE;
size_t HashTableStorage::defaultNumLocks = 193;
double StoredValue::mutation_mem_threshold = 0.9;
const int64_t StoredValue::state_deleted_key = -3;
const int64_t StoredValue::state_non_existent_key = -4;
const int64_t StoredValue::state_temp_init = -5;

static ssize_t prime_size_table[] = {
    3, 7, 13, 23, 47, 97, 193, 383, 769, 1531, 3079, 6143, 12289, 24571, 49157,
    98299, 196613, 393209, 786433, 1572869, 3145721, 6291449, 12582917,
    25165813, 50331653, 100663291, 201326611, 402653189, 805306357,
    1610612741, -1
};

bool StoredValue::ejectValue(HashTable &ht, item_eviction_policy_t policy) {
    if (eligibleForEviction(policy)) {
        reduceCacheSize(ht, value->length());
        markNotResident();
        value = NULL;
        return true;
    }
    return false;
}

void StoredValue::referenced() {
    if (nru > MIN_NRU_VALUE) {
        --nru;
    }
}

void StoredValue::setNRUValue(uint8_t nru_val) {
    if (nru_val <= MAX_NRU_VALUE) {
        nru = nru_val;
    }
}

uint8_t StoredValue::incrNRUValue() {
    uint8_t ret = MAX_NRU_VALUE;
    if (nru < MAX_NRU_VALUE) {
        ret = ++nru;
    }
    return ret;
}

uint8_t StoredValue::getNRUValue() {
    return nru;
}

bool StoredValue::unlocked_restoreValue(Item *itm, HashTable &ht) {
    if (isResident() || isDeleted()) {
        return false;
    }

    if (isTempInitialItem()) { // Regular item with the full eviction
        --ht.numTempItems;
        ht.incrementNumItems();
        newCacheItem = false; // set it back to false as we created a temp item
                              // by setting it to true when bg fetch is
                              // scheduled (full eviction mode).
    } else {
        --ht.numNonResidentItems;
    }

    if (isTempInitialItem()) {
        cas = itm->getCas();
        flags = itm->getFlags();
        exptime = itm->getExptime();
        revSeqno = itm->getRevSeqno();
        bySeqno = itm->getBySeqno();
        nru = INITIAL_NRU_VALUE;
    }
    deleted = false;
    conflictResMode = itm->getConflictResMode();
    value = itm->getValue();
    increaseCacheSize(ht, value->length());
    return true;
}

bool StoredValue::unlocked_restoreMeta(Item *itm, ENGINE_ERROR_CODE status,
                                       HashTable &ht) {
    if (!isTempInitialItem()) {
        return true;
    }

    switch(status) {
    case ENGINE_SUCCESS:
        cas = itm->getCas();
        flags = itm->getFlags();
        exptime = itm->getExptime();
        revSeqno = itm->getRevSeqno();
        if (itm->isDeleted()) {
            setStoredValueState(state_deleted_key);
        } else { // Regular item with the full eviction
            --ht.numTempItems;
            ht.incrementNumItems();
            ++ht.numNonResidentItems;
            bySeqno = itm->getBySeqno();
            newCacheItem = false; // set it back to false as we created a temp
                                  // item by setting it to true when bg fetch is
                                  // scheduled (full eviction mode).
        }
        if (nru == MAX_NRU_VALUE) {
            nru = INITIAL_NRU_VALUE;
        }
        conflictResMode = itm->getConflictResMode();
        return true;
    case ENGINE_KEY_ENOENT:
        setStoredValueState(state_non_existent_key);
        return true;
    default:
        LOG(EXTENSION_LOG_WARNING,
            "The underlying storage returned error %d for get_meta\n", status);
        return false;
    }
}

bool HashTable::unlocked_ejectItem(StoredValue*& vptr,
                                   item_eviction_policy_t policy) {
    cb_assert(vptr);

    if (policy == VALUE_ONLY) {
        bool rv = vptr->ejectValue(*this, policy);
        if (rv) {
            ++stats.numValueEjects;
            ++numNonResidentItems;
            ++numEjects;
            return true;
        } else {
            ++stats.numFailedEjects;
            return false;
        }
    } else { // full eviction.
        if (vptr->eligibleForEviction(policy)) {
            StoredValue::reduceMetaDataSize(*this, stats,
                                            vptr->metaDataSize());
            StoredValue::reduceCacheSize(*this, vptr->size());

            int bucket_num = getBucketForHash(hash(vptr));
            StoredValue *v = getBucketHead(bucket_num);
            // Remove the item from the hash table.
            if (v == vptr) {
                setBucketHead(bucket_num, v->next);
            } else {
                while (v->next) {
                    if (v->next == vptr) {
                        v->next = v->next->next;
                        break;
                    } else {
                        v = v->next;
                    }
                }
            }

            if (vptr->isResident()) {
                ++stats.numValueEjects;
            }
            if (!vptr->isResident() && !v->isTempItem()) {
                --numNonResidentItems; // Decrement because the item is
                                       // fully evicted.
            }
            decrementNumItems(); // Decrement because the item is fully evicted.
            ++numEjects;
            updateMaxDeletedRevSeqno(vptr->getRevSeqno());

            delete vptr; // Free the item.
            vptr = NULL;
            return true;
        } else {
            ++stats.numFailedEjects;
            return false;
        }
    }
}

mutation_type_t HashTable::insert(Item &itm, item_eviction_policy_t policy,
                                  bool eject, bool partial) {
    cb_assert(isActive());
    if (!StoredValue::hasAvailableSpace(stats, itm)) {
        return NOMEM;
    }

    int bucket_num(0);
    LockHolder lh = getLockedBucket(itm.getItemKey(), &bucket_num);
    StoredValue *v = unlocked_find(itm.getItemKey(), bucket_num, true, false);

    if (v == NULL) {
        v = valFact(itm, getBucketHead(bucket_num), *this);
        v->markClean();
        if (partial) {
            v->markNotResident();
            ++numNonResidentItems;
        }
        setBucketHead(bucket_num, v);
        incrementNumItems();
        v->setNewCacheItem(false);
    } else {
        if (partial) {
            // We don't have a better error code ;)
            return INVALID_CAS;
        }

        // Verify that the CAS isn't changed
        if (v->getCas() != itm.getCas()) {
            if (v->getCas() == 0) {
                v->cas = itm.getCas();
                v->flags = itm.getFlags();
                v->exptime = itm.getExptime();
                v->revSeqno = itm.getRevSeqno();
            } else {
                return INVALID_CAS;
            }
        }

        if (!v->isResident() && !v->isDeleted() && !v->isTempItem()) {
            --numNonResidentItems;
        }

        if (v->isTempItem()) {
            --numTempItems;
            incrementNumItems();
            ++numTotalItems;
        }

        v->setValue(const_cast<Item&>(itm), *this, true);
    }

    v->markClean();

    if (eject && !partial) {
        unlocked_ejectItem(v, policy);
    }

    return NOT_FOUND;
}

static inline size_t getDefault(size_t x, size_t d) {
    return x == 0 ? d : x;
}

size_t HashTableStorage::getNumBuckets(size_t n) {
    return getDefault(n, defaultNumBuckets);
}

size_t HashTableStorage::getNumLocks(size_t n) {
    return getDefault(n, defaultNumLocks);
}

/**
 * Set the default number of hashtable buckets.
 */
void HashTableStorage::setDefaultNumBuckets(size_t to) {
    if (to != 0) {
        defaultNumBuckets = to;
    }
}

/**
 * Set the default number of hashtable locks.
 */
void HashTableStorage::setDefaultNumLocks(size_t to) {
    if (to != 0) {
        defaultNumLocks = to;
    }
}

HashTableStatVisitor HashTableStorage::clearBucketUnlocked(bucket_id_t deleteMe) {
    HashTableStatVisitor rv;

    // Iterate through the hash buckets looking for the specific items.
    for (size_t ii = 0; ii < getSize(); ii++) {
        StoredValue* v = getBucketHead(ii);
        StoredValue* preValue = nullptr;
        while (v) {
            if (v->getBucketId() == deleteMe) {
                if (preValue) {
                    StoredValue* deleteV = v;
                    preValue->next = v->next;
                    v = v->next;
                    decrementNumItems();
                    rv.visit(deleteV);
                    delete deleteV;
                } else {
                    // The head node is a match
                    StoredValue* deleteV = v;
                    v = v->next;
                    // new head node
                    setBucketHead(ii, v);
                    decrementNumItems();
                    rv.visit(deleteV);
                    delete deleteV;
                }
            } else {
                preValue = v;
                v = v->next;
            }
        }
    }

    return rv;
}

HashTableStatVisitor HashTable::clear() {

    MultiLockHolder mlh(getMutexes(), getNumLocks());

    // clear this bucket
    HashTableStatVisitor rv = storage->clearBucketUnlocked(bucketId);

    stats.currentSize.fetch_sub(rv.memSize - rv.valSize);
    cb_assert(stats.currentSize.load() < GIGANTOR);

    numTotalItems.store(0);
    numItems.store(0);
    numTempItems.store(0);
    numNonResidentItems.store(0);
    memSize.store(0);
    cacheSize.store(0);

    return rv;
}

//
// Clear out the hashtable storage (everything is deleted)
//
void HashTableStorage::clear() {
    MultiLockHolder mlh(getMutexes(), getNumLocks());

    for (size_t ii = 0; ii < getSize(); ii++) {
        while (getBucketHead(ii)) {
            StoredValue *v = getBucketHead(ii);
            setBucketHead(ii, v->next);
            delete v;
        }
    }

    numItems.store(0);
}

static size_t distance(size_t a, size_t b) {
    return std::max(a, b) - std::min(a, b);
}

static size_t nearest(size_t n, size_t a, size_t b) {
    return (distance(n, a) < distance(b, n)) ? a : b;
}

static bool isCurrently(size_t size, ssize_t a, ssize_t b) {
    ssize_t current(static_cast<ssize_t>(size));
    return (current == a || current == b);
}

// TYNSET: remove this at some point, only HashTableStorage should attempt the resize
// requires the resize task moving to be a pool owned task
void HashTable::resize() {
    if (isActive()) {
        if (storage->resize()) {
            ++numResizes; //TYNSET: sort of meaningless when a pooled task
        }
    }
}

void HashTable::resize(size_t to) {
    if (isActive()) {
        if (storage->resize(to)) {
            ++numResizes;
        }
    }
}

bool HashTableStorage::resize() {
    size_t ni = numItems;
    int i(0);
    size_t new_size(0);

    // Figure out where in the prime table we are.
    ssize_t target(static_cast<ssize_t>(ni));
    for (i = 0; prime_size_table[i] > 0 && prime_size_table[i] < target; ++i) {
        // Just looking...
    }

    if (prime_size_table[i] == -1) {
        // We're at the end, take the biggest
        new_size = prime_size_table[i-1];
    } else if (prime_size_table[i] < static_cast<ssize_t>(defaultNumBuckets)) {
        // Was going to be smaller than the configured ht_size.
        new_size = defaultNumBuckets;
    } else if (isCurrently(size, prime_size_table[i-1], prime_size_table[i])) {
        // If one of the candidate sizes is the current size, maintain
        // the current size in order to remain stable.
        new_size = getSize();
    } else {
        // Somewhere in the middle, use the one we're closer to.
        new_size = nearest(ni, prime_size_table[i-1], prime_size_table[i]);
    }

    return resize(new_size);
}

bool HashTableStorage::resize(size_t newSize) {
    // Due to the way hashing works, we can't fit anything larger than
    // an int.
    if (newSize > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return false;
    }

    // Don't resize to the same size, either.
    if (newSize == getSize()) {
        return false;
    }

    MultiLockHolder mlh(getMutexes(), getNumLocks());

    if (visitors.load() > 0) {
        // Do not allow a resize while any visitors are actually
        // processing.  The next attempt will have to pick it up.  New
        // visitors cannot start doing meaningful work (we own all
        // locks at this point).
        return false;
    }

    // Get a place for the new items.
    StoredValue **newValues = static_cast<StoredValue**>(calloc(newSize,
                                                        sizeof(StoredValue*)));

    // If we can't allocate memory, don't move stuff around.
    if (!newValues) {
        return false;
    }

    // Set the new size so all the hashy stuff works.
    size_t oldSize = getSize();
    setSize(newSize);

    // Move existing records into the new space.
    for (size_t i = 0; i < oldSize; i++) {
        while (getBucketHead(i)) {
            StoredValue *v = getBucketHead(i);
            setBucketHead(i, v->next);

            int newBucket = getBucketForHash(HashTable::hash(v));
            v->next = newValues[newBucket];
            newValues[newBucket] = v;
        }
    }

    // values still points to the old (now empty) table.
    free(hashBuckets);
    hashBuckets = newValues;

    // TYNSET: resize() is not accounting in any stats for memorySize() and the change in memorySize()
    // As we cannot account the HashTable size to a bucket
    // Old code would also use VBucket constructor to capture intial ::memorySize in bucket stats.
    return true;
}

void HashTable::visit(HashTableVisitor &visitor) {
    if ((numItems.load() + numTempItems.load()) == 0 || !isActive()) {
        return;
    }
    VisitorTracker vt = storage->getNewVisitorTracker();
    bool aborted = !visitor.shouldContinue();
    size_t visited = 0;
    for (int l = 0; isActive() && !aborted && l < static_cast<int>(getNumLocks());
         l++) {
        LockHolder lh(getMutex(l));
        for (int i = l; i < static_cast<int>(getSize()); i+= getNumLocks()) {
            cb_assert(l == mutexForBucket(i));
            StoredValue *v = getBucketHead(i);
            cb_assert(v == NULL || i == getBucketForHash(hash(v)));
            while (v) {
                StoredValue *tmp = v->next;
                visitor.visit(v);
                v = tmp;
            }
            ++visited;
        }
        lh.unlock();
        aborted = !visitor.shouldContinue();
    }
    cb_assert(aborted || visited == getSize());
}

void HashTable::visitDepth(HashTableDepthVisitor &visitor) {
    if (numItems.load() == 0 || !isActive()) {
        return;
    }
    size_t visited = 0;
    VisitorTracker vt = storage->getNewVisitorTracker();

    for (int l = 0; l < static_cast<int>(getNumLocks()); l++) {
        LockHolder lh(getMutex(l));
        for (int i = l; i < static_cast<int>(getSize()); i+= getNumLocks()) {
            size_t depth = 0;
            StoredValue *p = getBucketHead(i);
            cb_assert(p == NULL || i == getBucketForHash(hash(p)));
            size_t mem(0);
            while (p) {
                depth++;
                mem += p->size();
                p = p->next;
            }
            visitor.visit(i, depth, mem);
            ++visited;
        }
    }

    cb_assert(visited == getSize());
}

HashTableStorage::Position
HashTableStorage::pauseResumeVisit(PauseResumeHashTableVisitor& visitor,
                                   Position& start_pos) {
    if (numItems.load() == 0) {
        // Nothing to visit
        return endPosition();
    }

    bool paused = false;
    VisitorTracker vt = getNewVisitorTracker();

    // Start from the requested lock number if in range.
    size_t lock = (start_pos.lock < getNumLocks()) ? start_pos.lock : 0;
    size_t hash_bucket = 0;

    for (; !paused && lock < getNumLocks(); lock++) {
        LockHolder lh(getMutex(lock));

        // If the bucket position is *this* lock, then start from the
        // recorded bucket (as long as we haven't resized).
        hash_bucket = lock;
        if (start_pos.lock == lock &&
            start_pos.ht_size == getSize() &&
            start_pos.hash_bucket < getSize()) {
            hash_bucket = start_pos.hash_bucket;
        }

        // Iterate across all values in the hash buckets owned by this lock.
        // Note: we don't record how far into the bucket linked-list we
        // pause at; so any restart will begin from the next bucket.
        for (; !paused && hash_bucket < getSize(); hash_bucket += getNumLocks()) {
            StoredValue *v = getBucketHead(hash_bucket);
            while (!paused && v) {
                StoredValue *tmp = v->next;
                paused = !visitor.visit(*v);
                v = tmp;
            }
        }

        // If the visitor paused us before we visited all hash buckets owned
        // by this lock, we don't want to skip the remaining hash buckets, so
        // stop the outer for loop from advancing to the next lock.
        if (paused && hash_bucket < getSize()) {
            break;
        }

        // Finished all buckets owned by this lock. Set hash_bucket to 'size'
        // to give a consistent marker for "end of lock".
        hash_bucket = getSize();
    }

    // Return the *next* location that should be visited.
    return HashTableStorage::Position(getSize(), lock, hash_bucket);
}

HashTableStorage::Position HashTableStorage::endPosition() const  {
    return HashTableStorage::Position(getSize(), getNumLocks(), getSize());
}

add_type_t HashTable::unlocked_add(int &bucket_num,
                                   StoredValue*& v,
                                   const Item &val,
                                   item_eviction_policy_t policy,
                                   bool isDirty,
                                   bool storeVal,
                                   bool maybeKeyExists,
                                   bool isReplication) {
    add_type_t rv = ADD_SUCCESS;
    if (v && !v->isDeleted() && !v->isExpired(ep_real_time()) &&
       !v->isTempItem()) {
        rv = ADD_EXISTS;
    } else {
        Item &itm = const_cast<Item&>(val);
        if (!StoredValue::hasAvailableSpace(stats, itm,
                                            isReplication)) {
            return ADD_NOMEM;
        }

        if (v) {
            if (v->isTempInitialItem() && policy == FULL_EVICTION) {
                // Need to figure out if an item exists on disk
                return ADD_BG_FETCH;
            }

            rv = (v->isDeleted() || v->isExpired(ep_real_time())) ?
                                   ADD_UNDEL : ADD_SUCCESS;
            if (v->isTempItem()) {
                if (v->isTempDeletedItem()) {
                    itm.setRevSeqno(v->getRevSeqno() + 1);
                } else {
                    itm.setRevSeqno(getMaxDeletedRevSeqno() + 1);
                }
                --numTempItems;
                incrementNumItems();
                ++numTotalItems;
            }
            v->setValue(itm, *this, v->isTempItem() ? true : false);
            if (isDirty) {
                v->markDirty();
            } else {
                v->markClean();
            }
        } else {
            if (val.getBySeqno() != StoredValue::state_temp_init) {
                if (policy == FULL_EVICTION && maybeKeyExists) {
                    return ADD_TMP_AND_BG_FETCH;
                }
            }
            v = valFact(itm, getBucketHead(bucket_num), *this, isDirty);
            setBucketHead(bucket_num, v);

            if (v->isTempItem()) {
                ++numTempItems;
                rv = ADD_BG_FETCH;
            } else {
                incrementNumItems();
                ++numTotalItems;
            }

            /**
             * Possibly, this item is being recreated. Conservatively assign
             * it a seqno that is greater than the greatest seqno of all
             * deleted items seen so far.
             */
            uint64_t seqno = 0;
            if (!v->isTempItem()) {
                seqno = getMaxDeletedRevSeqno() + 1;
            } else {
                seqno = getMaxDeletedRevSeqno();
            }
            v->setRevSeqno(seqno);
            itm.setRevSeqno(seqno);
        }
        if (!storeVal) {
            unlocked_ejectItem(v, policy);
        }
        if (v && v->isTempItem()) {
            v->markNotResident();
            v->setNRUValue(MAX_NRU_VALUE);
        }
    }

    return rv;
}

add_type_t HashTable::unlocked_addTempItem(int &bucket_num,
                                           const ItemKey &key,
                                           item_eviction_policy_t policy,
                                           bool isReplication) {

    cb_assert(isActive());
    uint8_t ext_meta[1];
    uint8_t ext_len = EXT_META_LEN;
    *(ext_meta) = PROTOCOL_BINARY_RAW_BYTES;

    Item itm(key, /*flags*/0, /*exp*/0, /*data*/NULL,
             /*size*/0, ext_meta, ext_len, 0, StoredValue::state_temp_init);

    // if a temp item for a possibly deleted, set it non-resident by resetting
    // the value cuz normally a new item added is considered resident which
    // does not apply for temp item.
    StoredValue* v = NULL;
    return unlocked_add(bucket_num, v, itm, policy,
                        false,  // isDirty
                        true,   // storeVal
                        true,
                        isReplication);
}

void StoredValue::setMutationMemoryThreshold(double memThreshold) {
    if (memThreshold > 0.0 && memThreshold <= 1.0) {
        mutation_mem_threshold = memThreshold;
    }
}

void StoredValue::increaseCacheSize(HashTable &ht, size_t by) {
    ht.cacheSize.fetch_add(by);
    cb_assert(ht.cacheSize.load() < GIGANTOR);
    ht.memSize.fetch_add(by);
    cb_assert(ht.memSize.load() < GIGANTOR);
}

void StoredValue::reduceCacheSize(HashTable &ht, size_t by) {
    ht.cacheSize.fetch_sub(by);
    cb_assert(ht.cacheSize.load() < GIGANTOR);
    ht.memSize.fetch_sub(by);
    cb_assert(ht.memSize.load() < GIGANTOR);
}

void StoredValue::increaseMetaDataSize(HashTable &ht, EPStats &st, size_t by) {
    ht.metaDataMemory.fetch_add(by);
    cb_assert(ht.metaDataMemory.load() < GIGANTOR);
    st.currentSize.fetch_add(by);
    cb_assert(st.currentSize.load() < GIGANTOR);
}

void StoredValue::reduceMetaDataSize(HashTable &ht, EPStats &st, size_t by) {
    ht.metaDataMemory.fetch_sub(by);
    cb_assert(ht.metaDataMemory.load() < GIGANTOR);
    st.currentSize.fetch_sub(by);
    cb_assert(st.currentSize.load() < GIGANTOR);
}

/**
 * Is there enough space for this thing?
 */
bool StoredValue::hasAvailableSpace(EPStats &st, const Item &itm,
                                    bool isReplication) {
    double newSize = static_cast<double>(st.getTotalMemoryUsed() +
                                         sizeof(StoredValue) + itm.getNKey());
    double maxSize = static_cast<double>(st.getMaxDataSize());
    if (isReplication) {
        return newSize <= (maxSize * st.tapThrottleThreshold);
    } else {
        return newSize <= (maxSize * mutation_mem_threshold);
    }
}

Item* StoredValue::toItem(bool lck, uint16_t vbucket) const {
    // create an ItemKey from our StoredValueKey
    ItemKey itemKey(key.getKey(), key.getKeyLen(), key.getBucketId());
    Item* itm = new Item(itemKey, getFlags(), getExptime(), value,
                         lck ? static_cast<uint64_t>(-1) : getCas(),
                         bySeqno, vbucket, getRevSeqno());

    itm->setNRUValue(nru);

    if (deleted) {
        itm->setDeleted();
    }

    itm->setConflictResMode(
          static_cast<enum conflict_resolution_mode>(conflictResMode));

    return itm;
}

void StoredValue::reallocate() {
    // Allocate a new Blob for this stored value; copy the existing Blob to
    // the new one and free the old.
    value_t new_val(Blob::Copy(*value));
    value.reset(new_val);
}

Item *HashTable::getRandomKeyFromSlot(int slot) {
    LockHolder lh = getLockedBucket(slot);
    StoredValue *v = getBucketHead(slot);

    while (v) {
        if (!v->isTempItem() && !v->isDeleted() && v->isResident()) {
            return v->toItem(false, 0);
        }
        v = v->next;
    }

    return NULL;
}

Item* HashTable::getRandomKey(long rnd) {
    /* Try to locate a partition */
    size_t start = rnd % getSize();
    size_t curr = start;
    Item *ret;

    do {
        ret = getRandomKeyFromSlot(curr++);
        if (curr == getSize()) {
            curr = 0;
        }
    } while (ret == NULL && curr != start);

    return ret;
}

std::ostream& operator<<(std::ostream& os, const HashTableStorage::Position& pos) {
    os << "{lock:" << pos.lock << " bucket:" << pos.hash_bucket << "/" << pos.ht_size << "}";
    return os;
}
