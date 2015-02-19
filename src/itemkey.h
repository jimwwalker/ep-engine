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

/**
    The key for an Item

    Encapsulates all data used to identify an item.

    Made up of the raw key-bytes obtained by the memcached protocol commands and a bucket index.

    When accessing the hashtable *all* of the data is used to obtain the hash-table location.
*/
class ItemKey {

    /**
        HashableKey lays out all ItemKey data required for hashing.
        The data is sequential in memory and may actually be bigger than sizeof(HashableKey)
    **/
    class HashableKey {
    public:
        HashableKey(const char* key, const size_t len, bucket_id_t id) : bucketId(id) {
            std::memcpy(keyBytes, key, len);
            keyBytes[len] = 0; // for safe printing as a C string
        }

        /**
            Return the number of bytes the hashable key will occupy
        **/
        static size_t getHashableKeySize(size_t len) {
            return sizeof(bucketId) + (len);
        }

        bucket_id_t bucketId;
        char keyBytes[1];
    };

public:

    // TYNSET: facecafe is temporary until all constructors can find the bucket_id
    ItemKey(const char* k, size_t len, bucket_id_t id = 0xfacecafe) : keyLen(len) {
        hashableKeyLen = HashableKey::getHashableKeySize(len);
        key.reset(new (::operator new(hashableKeyLen + 1))HashableKey(k, len, id));
        // cb_assert(id != 0xfacecafe);
    }

    // TYNSET: To be deleted. CacheCallback currently dependent on this, force to be broken
    ItemKey() : ItemKey("broken", 6) {

    }

    /*
        TYNSET DELETE THIS ONE?
    */
    ItemKey(std::string k, bucket_id_t id) : ItemKey(k.c_str(), k.length(), id) {

    }

    /*
        ItemKey copy constructor.
        Clones orginal key, however underlying key allocation is shared (shared_ptr)
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
        return key->keyBytes;
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
        return key->bucketId;
    }

    bool operator==(const ItemKey &other) const {
        return other.getHashKeyLen() == getHashKeyLen() &&
        (memcmp(other.getHashKey(), getHashKey(), getHashKeyLen()) == 0);
    }

private:
    size_t hashableKeyLen;
    size_t keyLen;
    std::shared_ptr<HashableKey> key;
};

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