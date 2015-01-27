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

#include <ep.h>
#include <item.h>
#include <signal.h>
#include <stats.h>

#include <algorithm>
#include <limits>

#include "threadtests.h"

#ifdef _MSC_VER
#define alarm(a)
#endif

#define BUCKET_ID (0)

time_t time_offset;

/* HashTable requires the callers stats */
EPStats epstats;

extern "C" {
    static rel_time_t basic_current_time(void) {
        return 0;
    }

    rel_time_t (*ep_current_time)() = basic_current_time;

    time_t ep_real_time() {
        return time(NULL) + time_offset;
    }
}

EPStats global_stats;

class Counter : public HashTableVisitor {
public:

    size_t count;
    size_t deleted;

    Counter(bool v) : count(), deleted(), verify(v) {}

    void visit(StoredValue *v) {
        if (v->isDeleted()) {
            ++deleted;
        } else {
            ++count;
            if (verify) {
                const char* key = v->getKey();
                value_t val = v->getValue();
                cb_assert(memcmp(key, val->getData(), v->getKeyLen()) == 0);
            }
        }
    }
private:
    bool verify;
};

static int count(HashTable &h, bool verify=true) {
    Counter c(verify);
    h.visit(c);
    cb_assert(c.count + c.deleted == h.getNumItems());
    return c.count;
}

static void store(HashTable &h, ItemKey &k) {
    Item i(k, 0, 0, k.getKey(), k.getKeyLen());
    cb_assert(h.set(i) == WAS_CLEAN);
}

static void storeMany(HashTable &h, std::vector<ItemKey> &keys) {
    std::vector<ItemKey>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        ItemKey key = *it;
        store(h, key);
    }
}

static void addMany(HashTable &h, std::vector<ItemKey> &keys,
                    add_type_t expect) {
    std::vector<ItemKey>::iterator it;
    item_eviction_policy_t policy = VALUE_ONLY;
    for (it = keys.begin(); it != keys.end(); ++it) {
        ItemKey k = *it;
        Item i(k, 0, 0, k.getKey(), k.getKeyLen());
        add_type_t v = h.add(i, policy);
        cb_assert(expect == v);
    }
}

template <typename T>
static const char *toString(add_type_t a) {
    switch(a) {
    case ADD_SUCCESS: return "add_success";
    case ADD_NOMEM: return "add_nomem";
    case ADD_EXISTS: return "add_exists";
    case ADD_UNDEL: return "add_undel";
    case ADD_TMP_AND_BG_FETCH: return "add_tmp_and_bg_fetch";
    case ADD_BG_FETCH: return "add_bg_fetch";
    }
    abort();
    return NULL;
}

template <typename T>
void assertEquals(T a, T b) {
    if (a != b) {
        std::cerr << "Expected " << toString<T>(a)
                  << " got " << toString<T>(b) << std::endl;
        abort();
    }
}

static void add(HashTable &h, const ItemKey &k, add_type_t expect,
                int expiry=0) {
    Item i(k, 0, expiry, k.getKey(), k.getKeyLen());
    item_eviction_policy_t policy = VALUE_ONLY;
    add_type_t v = h.add(i, policy);
    assertEquals(expect, v);
}

static std::vector<ItemKey> generateKeys(int num, int start=0) {
    std::vector<ItemKey> rv;

    for (int i = start; i < num; i++) {
        char buf[64];
        int len = snprintf(buf, sizeof(buf), "key%d", i);
        ItemKey key(buf, len, BUCKET_ID);
        rv.push_back(key);
    }

    return rv;
}

// ----------------------------------------------------------------------
// Actual tests below.
// ----------------------------------------------------------------------

static void testHashSize() {
    HashTableStorage storage;
    HashTable h(BUCKET_ID, &storage, epstats);
    cb_assert(count(h) == 0);

    ItemKey k("testkey", 7, 0);
    store(h, k);

    cb_assert(count(h) == 1);
}

static void testHashSizeTwo() {
    HashTableStorage storage;
    HashTable h(BUCKET_ID, &storage, epstats);
    cb_assert(count(h) == 0);

    std::vector<ItemKey> keys = generateKeys(5);
    storeMany(h, keys);
    cb_assert(count(h) == 5);

    h.clear();
    cb_assert(count(h) == 0);
}

static void testReverseDeletions() {
    alarm(20);
    size_t initialSize = global_stats.currentSize.load();
    HashTableStorage storage(5, 1);
    HashTable h(BUCKET_ID, &storage, epstats);
    cb_assert(count(h) == 0);
    const int nkeys = 10000;

    std::vector<ItemKey> keys = generateKeys(nkeys);
    storeMany(h, keys);
    cb_assert(count(h) == nkeys);

    std::reverse(keys.begin(), keys.end());

    std::vector<ItemKey>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        ItemKey key = *it;
        h.del(key);
    }

    cb_assert(count(h) == 0);
    cb_assert(global_stats.currentSize.load() == initialSize);
}

static void testForwardDeletions() {
    alarm(20);
    size_t initialSize = global_stats.currentSize.load();
    HashTableStorage storage(5, 1);
    HashTable h(BUCKET_ID, &storage, epstats);
    cb_assert(h.getSize() == 5);
    cb_assert(h.getNumLocks() == 1);
    cb_assert(count(h) == 0);
    const int nkeys = 10000;

    std::vector<ItemKey> keys = generateKeys(nkeys);
    storeMany(h, keys);
    cb_assert(count(h) == nkeys);

    std::vector<ItemKey>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        ItemKey key = *it;
        h.del(key);
    }

    cb_assert(count(h) == 0);
    cb_assert(global_stats.currentSize.load() == initialSize);
}

static void verifyFound(HashTable &h, const std::vector<ItemKey> &keys) {
    ItemKey missingKey("aMissingKey", 11, 0);;
    cb_assert(h.find(missingKey) == NULL);

    std::vector<ItemKey>::const_iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        ItemKey key = *it;
        cb_assert(h.find(key));
    }
}

static void testFind(HashTable &h) {
    const int nkeys = 5000;

    std::vector<ItemKey> keys = generateKeys(nkeys);
    storeMany(h, keys);

    verifyFound(h, keys);
}

static void testFind() {
    HashTableStorage storage(5, 1);
    HashTable h(BUCKET_ID, &storage, epstats);
    testFind(h);
}

static void testAddExpiry() {
    HashTableStorage storage(5, 1);
    HashTable h(BUCKET_ID, &storage, epstats);
    ItemKey k("aKey", 4, 0);

    add(h, k, ADD_SUCCESS, ep_real_time() + 5);
    add(h, k, ADD_EXISTS, ep_real_time() + 5);

    StoredValue *v = h.find(k);
    cb_assert(v);
    cb_assert(!v->isExpired(ep_real_time()));
    cb_assert(v->isExpired(ep_real_time() + 6));

    time_offset += 6;
    cb_assert(v->isExpired(ep_real_time()));

    add(h, k, ADD_UNDEL, ep_real_time() + 5);
    cb_assert(v);
    cb_assert(!v->isExpired(ep_real_time()));
    cb_assert(v->isExpired(ep_real_time() + 6));
}

static void testResize() {
    HashTableStorage storage(5, 3);
    HashTable h(BUCKET_ID, &storage, epstats);

    std::vector<ItemKey> keys = generateKeys(5000);
    storeMany(h, keys);

    verifyFound(h, keys);

    h.resize(6143);
    cb_assert(h.getSize() == 6143);
    cb_assert(storage.getSize() == 6143); // validate storage resized

    verifyFound(h, keys);

    h.resize(769);
    cb_assert(h.getSize() == 769);
    cb_assert(storage.getSize() == 769);

    verifyFound(h, keys);

    h.resize(static_cast<size_t>(std::numeric_limits<int>::max()) + 17);
    cb_assert(h.getSize() == 769);

    verifyFound(h, keys);
}

class AccessGenerator : public Generator<bool> {
public:

    AccessGenerator(const std::vector<ItemKey> &k,
                    HashTable &h) : keys(k), ht(h), size(10000) {
        std::random_shuffle(keys.begin(), keys.end());
    }

    ~AccessGenerator(){}

    bool operator()() {
        std::vector<ItemKey>::iterator it;
        for (it = keys.begin(); it != keys.end(); ++it) {
            if (rand() % 111 == 0) {
                resize();
            }
            ht.del(*it);
        }
        return true;
    }

private:

    void resize() {
        ht.resize(size);
        size = size == 10000 ? 30000 : 10000;
    }

    std::vector<ItemKey>  keys;
    HashTable                &ht;
    size_t                    size;
};

static void testConcurrentAccessResize() {
    HashTableStorage storage(5, 3);
    HashTable h(BUCKET_ID, &storage, epstats);

    std::vector<ItemKey> keys = generateKeys(20000);
    h.resize(keys.size());
    storeMany(h, keys);

    verifyFound(h, keys);

    srand(918475);
    AccessGenerator gen(keys, h);
    getCompletedThreads(16, &gen);
}

static void testAutoResize() {
    HashTableStorage storage(5, 3);
    HashTable h(BUCKET_ID, &storage, epstats);

    std::vector<ItemKey> keys = generateKeys(5000);
    storeMany(h, keys);

    verifyFound(h, keys);

    h.resize();
    cb_assert(h.getSize() == 6143);
    verifyFound(h, keys);
}

static void testAdd() {
    HashTableStorage storage(5, 1);
    HashTable h(BUCKET_ID, &storage, epstats);

    const int nkeys = 5000;

    std::vector<ItemKey> keys = generateKeys(nkeys);
    addMany(h, keys, ADD_SUCCESS);

    ItemKey missingKey("aMissingKey", 11, 0);
    cb_assert(h.find(missingKey) == NULL);

    std::vector<ItemKey>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        ItemKey key = *it;
        cb_assert(h.find(key));
    }

    addMany(h, keys, ADD_EXISTS);
    for (it = keys.begin(); it != keys.end(); ++it) {
        ItemKey key = *it;
        cb_assert(h.find(key));
    }

    // Verify we can readd after a soft deletion.
    cb_assert(h.softDelete(keys[0], 0) == WAS_DIRTY);
    cb_assert(h.softDelete(keys[0], 0) == NOT_FOUND);
    cb_assert(!h.find(keys[0]));
    cb_assert(count(h) == nkeys - 1);

    Item i(keys[0], 0, 0, "newtest", 7);
    item_eviction_policy_t policy = VALUE_ONLY;
    cb_assert(h.add(i, policy) == ADD_UNDEL);
    cb_assert(count(h, false) == nkeys);
}

static void testDepthCounting() {
    HashTableStorage storage(5, 1);
    HashTable h(BUCKET_ID, &storage, epstats);

    const int nkeys = 5000;

    std::vector<ItemKey> keys = generateKeys(nkeys);
    storeMany(h, keys);

    HashTableDepthStatVisitor depthCounter;
    h.visitDepth(depthCounter);
    // std::cout << "Max depth:  " << depthCounter.maxDepth << std::endl;
    cb_assert(depthCounter.max > 1000);
}

static void testPoisonKey() {
    const char poison[] = "A\\NROBs_oc)$zqJ1C.9?XU}Vn^(LW\"`+K/4lykF[ue0{ram;fv"
                          "Id6h=p&Zb3T~SQ]82'ixDP";
    ItemKey k(poison, sizeof(poison)-1, 0);

    HashTableStorage storage(5, 1);
    HashTable h(BUCKET_ID, &storage, epstats);

    store(h, k);
    cb_assert(count(h) == 1);
}

static void testSizeStats() {
    global_stats.reset();
    HashTableStorage storage(5, 1);
    HashTable ht(BUCKET_ID, &storage, epstats);
    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    size_t initialSize = global_stats.currentSize.load();

    const ItemKey k("somekey", 7, 0);
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    cb_assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    cb_assert(ht.set(i) == WAS_CLEAN);

    ht.del(k);

    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    cb_assert(initialSize == global_stats.currentSize.load());

    free(someval);
}

static void testSizeStatsFlush() {
    global_stats.reset();
    HashTableStorage storage(5, 1);
    HashTable ht(BUCKET_ID, &storage, epstats);
    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    size_t initialSize = global_stats.currentSize.load();

    const ItemKey k("somekey", 7, 0);
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    cb_assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    cb_assert(ht.set(i) == WAS_CLEAN);

    ht.clear();

    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    cb_assert(initialSize == global_stats.currentSize.load());

    free(someval);
}

static void testSizeStatsSoftDel() {
    global_stats.reset();
    HashTableStorage storage(5, 1);
    HashTable ht(BUCKET_ID, &storage, epstats);
    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    size_t initialSize = global_stats.currentSize.load();

    const ItemKey k("somekey", 7, 0);
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    cb_assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    cb_assert(ht.set(i) == WAS_CLEAN);

    cb_assert(ht.softDelete(k, 0) == WAS_DIRTY);
    ht.del(k);

    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    cb_assert(initialSize == global_stats.currentSize.load());

    free(someval);
}

static void testSizeStatsSoftDelFlush() {
    global_stats.reset();
    HashTableStorage storage(5, 1);
    HashTable ht(BUCKET_ID, &storage, epstats);
    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    size_t initialSize = global_stats.currentSize.load();

    const ItemKey k("somekey", 7, 0);
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    cb_assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    cb_assert(ht.set(i) == WAS_CLEAN);

    cb_assert(ht.softDelete(k, 0) == WAS_DIRTY);
    ht.clear();

    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    cb_assert(initialSize == global_stats.currentSize.load());

    free(someval);
}

static void testSizeStatsEject() {
    global_stats.reset();
    HashTableStorage storage(5, 1);
    HashTable ht(BUCKET_ID, &storage, epstats);
    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    size_t initialSize = global_stats.currentSize.load();

    const ItemKey k("somekey", 7, 0);
    ItemKey kstring(k);
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    cb_assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    cb_assert(ht.set(i) == WAS_CLEAN);

    item_eviction_policy_t policy = VALUE_ONLY;
    StoredValue *v(ht.find(kstring));
    cb_assert(v);
    v->markClean();
    cb_assert(ht.unlocked_ejectItem(v, policy));

    ht.del(k);

    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    cb_assert(initialSize == global_stats.currentSize.load());

    free(someval);
}

static void testSizeStatsEjectFlush() {
    global_stats.reset();
    HashTableStorage storage(5, 1);
    HashTable ht(BUCKET_ID, &storage, epstats);
    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    size_t initialSize = global_stats.currentSize.load();

    ItemKey k("somekey", 7, 0);

    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    cb_assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    cb_assert(ht.set(i) == WAS_CLEAN);

    item_eviction_policy_t policy = VALUE_ONLY;
    StoredValue *v(ht.find(k));
    cb_assert(v);
    v->markClean();
    cb_assert(ht.unlocked_ejectItem(v, policy));

    ht.clear();

    cb_assert(ht.memSize.load() == 0);
    cb_assert(ht.cacheSize.load() == 0);
    cb_assert(initialSize == global_stats.currentSize.load());

    free(someval);
}

static void testItemAge() {
    // Setup
    HashTableStorage storage(5, 1);
    HashTable ht(BUCKET_ID, &storage, epstats);
    ItemKey key("key", 3, 0);
    Item item(key, 0, 0, "value", strlen("value"));
    cb_assert(ht.set(item) == WAS_CLEAN);

    // Test
    StoredValue* v(ht.find(key));
    cb_assert(v->getValue()->getAge() == 0);
    v->getValue()->incrementAge();
    cb_assert(v->getValue()->getAge() == 1);

    // Check saturation of age.
    for (int ii = 0; ii < 300; ii++) {
        v->getValue()->incrementAge();
    }
    cb_assert(v->getValue()->getAge() == 0xff);

    // Check reset of age after reallocation.
    v->reallocate();
    cb_assert(v->getValue()->getAge() == 0);

    // Check changing age when new value is used.
    Item item2(key, 0, 0, "value2", strlen("value2"));
    item2.getValue()->incrementAge();
    v->setValue(item2, ht, false);
    cb_assert(v->getValue()->getAge() == 1);
}

static void testBucketIdSeparation() {
    // Setup
    HashTableStorage storage(5, 1);
    HashTable ht(BUCKET_ID, &storage, epstats);
    const char key[] = "bucket_key";
    std::string data = "bucket";

    for (int i = 0; i < 10; i++) {
        std::stringstream document;
        document << "bucket" << i;
        Item item(ItemKey(key, sizeof(key) -1, i), 0, 0, document.str().c_str(), document.str().length());
        cb_assert(ht.set(item) == WAS_CLEAN);
    }

    for (int i = 0; i < 10; i++) {
        std::stringstream document;
        document << "bucket" << i;
        StoredValue* sv = ht.find(ItemKey(key, sizeof(key) -1, i));
        cb_assert(sv);
        value_t v = sv->getValue();
        cb_assert(memcmp(document.str().c_str(), v->getData(), v->vlength()) == 0);
    }
}

static void testSharedStorage() {
    // Setup
    HashTableStorage storage(5, 1);
    HashTable ht1(0, &storage, epstats);
    HashTable ht2(1, &storage, epstats);
    HashTable ht3(2, &storage, epstats);
    HashTable ht4(3, &storage, epstats);

    const int n = 10;
    std::string keyRoot = "bucket_key";
    std::string data = "bucket";

    for (int i = 0; i < n; i++) {
        std::stringstream key;
        key << keyRoot << i;
        std::stringstream document;
        document << "bucket" << i;
        Item item1(ItemKey(key.str().c_str(), key.str().length(), 0), 0, 0, document.str().c_str(), document.str().length());
        Item item2(ItemKey(key.str().c_str(), key.str().length(), 1), 0, 0, document.str().c_str(), document.str().length());
        Item item3(ItemKey(key.str().c_str(), key.str().length(), 2), 0, 0, document.str().c_str(), document.str().length());
        Item item4(ItemKey(key.str().c_str(), key.str().length(), 3), 0, 0, document.str().c_str(), document.str().length());
        cb_assert(ht1.set(item1) == WAS_CLEAN);
        cb_assert(ht2.set(item2) == WAS_CLEAN);
        cb_assert(ht3.set(item3) == WAS_CLEAN);
        cb_assert(ht4.set(item4) == WAS_CLEAN);
    }

    // Storage should have twice the number of items as each hashtable reports
    cb_assert(storage.getNumItems() == n * 4);
    cb_assert(ht1.getNumItems() == n);
    cb_assert(ht2.getNumItems() == n);
    cb_assert(ht3.getNumItems() == n);
    cb_assert(ht4.getNumItems() == n);

    // clear should remove items from shared storage
    ht2.clear();
    // now 1/4 of thems will have been deleted
    cb_assert(storage.getNumItems() == n * 3);
    cb_assert(ht1.getNumItems() == n); // no change
    cb_assert(ht2.getNumItems() == 0); // should be 0
}

static void testSharedResize() {
    HashTableStorage storage(5, 3);
    HashTable h1(0, &storage, epstats);
    HashTable h2(1, &storage, epstats);

    // When two hashtables share the same underlying
    // storage they will affect each other with a resize()
    // the ht.resize path maybe removed for a logically cleaner
    // implementation

    h1.resize(6143);
    cb_assert(h1.getSize() == 6143);
    cb_assert(h2.getSize() == 6143);
    cb_assert(storage.getSize() == 6143);

    h2.resize(769);
    cb_assert(h1.getSize() == 769);
    cb_assert(h2.getSize() == 769);
    cb_assert(storage.getSize() == 769);
}

/* static storage for environment variable set by putenv().
 *
 * (This must be static as putenv() essentially 'takes ownership' of
 * the provided array, so it is unsafe to use an automatic variable.
 * However, if we use the result of malloc() (i.e. the heap) then
 * memory leak checkers (e.g. Valgrind) will report the memory as
 * leaked as it's impossible to free it).
 */
static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

int main() {
    putenv(allow_no_stats_env);
    global_stats.setMaxDataSize(64*1024*1024);
    HashTableStorage::setDefaultNumBuckets(3);
    alarm(60);
    testHashSize();
    testHashSizeTwo();
    testReverseDeletions();
    testForwardDeletions();
    testFind();
    testAdd();
    testAddExpiry();
    testDepthCounting();
    testPoisonKey();
    testResize();
    testConcurrentAccessResize();
    testAutoResize();
    testSizeStats();
    testSizeStatsFlush();
    testSizeStatsSoftDel();
    testSizeStatsSoftDelFlush();
    testSizeStatsEject();
    testSizeStatsEjectFlush();
    testItemAge();
    testBucketIdSeparation();
    testSharedStorage();
    testSharedResize();
    exit(0);
}
