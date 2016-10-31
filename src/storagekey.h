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

#pragma once

#include <memcached/engine.h>
#include <vector>

#include "ep_types.h"

class ProtocolKey;
class SerialisedStorageKey;
class SerialisedProtocolKey;

static inline uint32_t hashDocKey(const DocKey& key) {
    uint32_t h = 5381;

    h = ((h << 5) + h) ^ uint32_t(key.doc_namespace);

    for (size_t i = 0; i < key.size(); i++) {
        h = ((h << 5) + h) ^ uint32_t(key.data()[i]);
    }

    return h;
}

/*
 * StorageKey is a container used to represent a key inside our storage
 * mediums.
 */
class StorageKey : public DocKey {
public:

    StorageKey(const uint8_t* key, size_t nkey, DocNamespace docNamespace)
        : DocKey(nullptr, nkey, docNamespace),
          storage(nkey + terminatorBytes + namespaceBytes) {
        storage[0] = static_cast<uint8_t>(docNamespace);
        std::copy(key, key + nkey, &storage[1]);
        storage.back() = 0;
        buf = &storage.data()[1];
        // hmms with vector management now we have 2 sizes stored...
    }

    StorageKey(const DocKey key)
        : StorageKey(key.data(), key.size(), key.doc_namespace) {
    }

    StorageKey(const char* cString, DocNamespace docNamespace)
        : StorageKey(reinterpret_cast<const uint8_t*>(cString),
                     std::strlen(cString), docNamespace) {
    }

    StorageKey(const std::string& key, DocNamespace docNamespace)
        : StorageKey(reinterpret_cast<const uint8_t*>(key.c_str()),
                     key.size(), docNamespace) {
    }

    // Deserialise constructor - assumes namespace encoded in key
    // DocKey size is nkey - 1
    // DocNamespace is byte key[0]
    StorageKey(const uint8_t* key, size_t nkey)
        : DocKey(nullptr, nkey - 1, DocNamespace::DefaultCollection),
          storage(nkey + terminatorBytes) {
        std::copy(key, key + nkey, storage.data());
        storage.back() = 0;
        buf = &storage.data()[1];
        doc_namespace = static_cast<DocNamespace>(storage[0]);
    }

    /*
     * A StorageKey can be created from its serialised equivalent.
     */
    StorageKey(const SerialisedStorageKey& key);

    const uint8_t* data() const {
        return &storage.data()[1];
    }

    size_t size() const {
        return storage.size() - terminatorBytes - namespaceBytes;
    }

    DocNamespace getDocNamespace() const {
        return doc_namespace;
    }

    const uint8_t* jww_data() const {
        return storage.data();
    }

    size_t jww_size() const {
        return storage.size() - 1;
    }

    bool operator == (const StorageKey& rhs) const {
        return doc_namespace == rhs.doc_namespace && storage == rhs.storage;
    }

    bool operator != (const StorageKey& rhs) const {
        return !(*this == rhs);
    }

    bool operator < (const StorageKey& rhs) const {
        return doc_namespace <= rhs.doc_namespace && storage < rhs.storage;
    }

    bool operator > (const StorageKey& rhs) const {
        return doc_namespace > rhs.doc_namespace && storage > rhs.storage;
    }

    bool operator <= (const StorageKey& rhs) const {
        return doc_namespace <= rhs.doc_namespace && storage <= rhs.storage;
    }

    bool operator >= (const StorageKey& rhs) const {
        return doc_namespace >= rhs.doc_namespace && storage >= rhs.storage;
    }

    const DocKey getDocKey() const {
        return DocKey(storage.data(), storage.size(), doc_namespace);
    }

protected:

    static const size_t terminatorBytes = 1;
    static const size_t namespaceBytes = sizeof(std::underlying_type<DocNamespace>::type);
    std::vector<uint8_t> storage;
};


class MutationLogEntry;
class StoredValue;

/*
 * SerialisedStorageKey maintains the key in a single continuous allocation.
 *
 * A limited number of classes are friends and can directly construct this
 * key.
 */
class SerialisedStorageKey {
public:
    /*
     * The copy constructor is deleted.
     * Copying a SerialisedStorageKey is dangerous due to the bytes living
     * outside of the object.
     */
    SerialisedStorageKey(const SerialisedStorageKey &obj) = delete;

    const uint8_t* data() const {
        return bytes;
    }

    size_t size() const {
        return length;
    }

    DocNamespace getDocNamespace() const {
        return docNamespace;
    }

    bool operator == (const DocKey rhs) const {
        return size() == rhs.size() &&
               getDocNamespace() == rhs.doc_namespace &&
               std::memcmp(data(), rhs.data(), size()) == 0;
    }

    /*
     * Return how many bytes are (or should be) allocated to this object
     */
    size_t getObjectSize() const {
        return getObjectSize(length);
    }

    /*
     * Return how many bytes are needed to store a key of len.
     */
    static size_t getObjectSize(size_t len) {
        return sizeof(SerialisedStorageKey) + len - 1;
    }

    /*
     * Allocate the correct storage for nkey bytes and return the
     * SerialisedStorageKey as a unique_ptr
     */
    static std::unique_ptr<SerialisedStorageKey> make(const uint8_t* key,
                                                      size_t nkey,
                                                      DocNamespace docNamespace) {
        std::unique_ptr<SerialisedStorageKey>
            rval(reinterpret_cast<SerialisedStorageKey*>(new uint8_t[getObjectSize(nkey)]));
        new (rval.get()) SerialisedStorageKey(key, nkey, docNamespace);
        return rval;
    }

    static std::unique_ptr<SerialisedStorageKey> make(const StorageKey& key) {
        return make(key.data(), key.size(), key.getDocNamespace());
    }

    /*
     * Get a hash of the SerialisedStorageKey
     * @return the hash value
     */
    uint32_t hash() const {
        return hashDocKey(DocKey(data(), size(), getDocNamespace()));
    }

protected:

    /*
     * These following classes are "white-listed". They know how to allocate
     * and construct this object so are allowed direct access to the
     * constructors.
     */
    friend class MutationLogEntry;
    friend class StoredValue;

    SerialisedStorageKey() : length(0) {}

    /*
     * Copy-in c to c+n bytes.
     * n must be less than 255
     */
    SerialisedStorageKey(const uint8_t* c, uint8_t n, DocNamespace docNamespace) {
        if (n > std::numeric_limits<uint8_t>::max()) {
            throw std::length_error("SerialisedStorageKey size exceeded " +
                                    std::to_string(n));
        }
        length = n;
        std::memcpy(bytes, c, n);
        this->docNamespace = docNamespace;
    }

    /*
     * SerialisedStorageKey can be created from a StorageKey.
     */
    SerialisedStorageKey(const StorageKey& key)       : length(key.size()){
    docNamespace = key.doc_namespace;
    std::memcpy(bytes, key.data(), key.size());
}

    uint8_t length;
    DocNamespace docNamespace;
    uint8_t bytes[1]; // key bytes
};

inline StorageKey::StorageKey(const SerialisedStorageKey& key)
      : StorageKey(DocKey(key.data(), key.size(), key.getDocNamespace())) {}



/*
 * A hash function for StorageKey so they can be used in std::map and friends.
 */
namespace std {
    template<>
    struct hash<StorageKey> {
        std::size_t operator()(const StorageKey& s) const {
            return hashDocKey(s);
        }
    };
}
