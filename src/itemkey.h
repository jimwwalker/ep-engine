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

#include <string>
#include <cstring>
#include <memory>
#include <stddef.h>
#include <algorithm>
#include "memcached/types.h"
#include <iostream>

/*
    The key for an Item

    Encapsulates all data used to identify an item.

    Made up of the raw key-bytes obtained by the memcached protocol commands
    and a bucket index.

    When accessing the hashtable *all* of the data is used to obtain the
    hash-table location.
*/
class ItemKey {

    /*
        HashableKey lays out all ItemKey data required for hashing.
        The data is sequential in memory and may actually be bigger
        than sizeof(HashableKey)
    */
    class HashableKey {
    public:
        HashableKey(const char* key, const size_t len, bucket_id_t id) {
            (void)id;
            std::memcpy(keyBytes, key, len);
            // Force zero terminate for safe printing and comparisons
            keyBytes[len] = 0;
        }

        /*
            Retrieve how much compiler specified/pre-allocated
            storage exists for the key.
        */
        static size_t getKeyBytesAllocationSize() {
            return sizeof(keyBytes);
        }

        bucket_id_t getBucketId() const {
            return 0; // not stored yet, return 0
        }

        const char* getKeyBytes() const {
            return keyBytes;
        }

    private:
        char keyBytes[1];
    };

public:

    /*
        Construct an item key for the bucket (id)
    */
    ItemKey(const char* k, size_t len, bucket_id_t id)
      : keyLen(len) {
        size_t allocLen = ItemKey::getRequiredStorage(len);
        hashableKeyLen = allocLen - 1;
        key.reset(new (::operator new(allocLen))HashableKey(k, len, id));
    }

    ItemKey(std::string k, bucket_id_t id)
      : ItemKey(k.c_str(), k.length(), id) {
    }

    /*
        ItemKey copy constructor.

        Clones orginal key, however underlying key allocation is shared by the
        shared_ptr.
    */
    ItemKey(const ItemKey& k) {
        this->keyLen = k.keyLen;
        this->hashableKeyLen = k.hashableKeyLen;
        // note the shared_ptr here
        this->key = k.key;
    }

    ~ItemKey() {
        key.reset(); // relinquish reference to pointer
    }

    /*
        Return a pointer to the data which must be used for hashing.
    */
    const char* getHashKey() const {
        return reinterpret_cast<char*>(key.get());
    }

    /*
        Return the length of the data used for hashing.
    */
    size_t getHashKeyLen() const {
        return hashableKeyLen;
    }

    /*
        Return the key (as obtained by memcache protocol).
    */
    const char* getKey() const {
        return key->getKeyBytes();
    }

    /*
        Return the key length (as obtained by memcache protocol).
    */
    size_t getKeyLen() const {
        return keyLen;
    }

    /*
        Return the id of the bucket this key belongs to.
    */
    bucket_id_t getBucketId() const {
        return key->getBucketId();
    }

    bool operator==(const ItemKey &other) const {
        return getHashKeyLen() == other.getHashKeyLen() &&
            (memcmp(getHashKey(), other.getHashKey(), getHashKeyLen()) == 0);
    }

    bool operator!=(const ItemKey &other) const {
        return !(*this == other);
    }

    bool operator<(const ItemKey &other) const {
        // Compare using the longest key so that 'key9\0' and 'key99\0' are
        // correctly seen as different.
        // ItemHashKey zero terminates the data so we won't overflow on the
        // shorter key, we'll stop on the zero terminator compared against a
        // non-zero.
        return  memcmp(getHashKey(), other.getHashKey(),
                      std::max(getHashKeyLen(), other.getHashKeyLen())) < 0;
    }

    /*
        The amount of storage required for keyLen
    */
    static size_t getRequiredStorage(size_t keyLen) {
        size_t base = sizeof(HashableKey) + keyLen + 1;
        return base - HashableKey::getKeyBytesAllocationSize();
    }

    friend std::ostream& operator<<(std::ostream& os, const ItemKey& itemKey) {
        os << itemKey.getBucketId() << ":" << itemKey.getKey();
        return os;
    }

private:
    size_t hashableKeyLen;
    size_t keyLen;
    std::shared_ptr<HashableKey> key;
};

/**
    To allow use of ItemKey in STL containers which require a hash function
**/
struct ItemKeyHash
{
    size_t operator()(const ItemKey& k) const {
        size_t h = 0;
        for (size_t i = 0; i < k.getHashKeyLen(); i++) {
            h ^= std::hash<char>()(k.getHashKey()[i]);
        }
        return h;
  }
};