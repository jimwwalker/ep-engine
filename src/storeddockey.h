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

static inline uint32_t hashDocKey(const DocKey& key) {
    uint32_t h = 5381;

    h = ((h << 5) + h) ^ uint32_t(key.doc_namespace);

    for (size_t i = 0; i < key.size(); i++) {
        h = ((h << 5) + h) ^ uint32_t(key.data()[i]);
    }

    return h;
}

class SerialisedDocKey;

/*
 * StoredDocKey copies into its own storage the key data.
 *
 * Internally the key an n byte key is stored in a n+2 byte array.
 *  - We zero terminate so that data() is safe for printing as a c-string
 *  - We store the DocNamespace in byte 0 (duplicated in parent class DocKey).
 *    This is because StoredDocKey usually ends up being written to disk and
 *    the DocNamespace forms part of the on-disk key. Pre-allocating space
 *    for the DocNamespace means storage doesn't have to create a new buffer
 *    to store DocNamespace and key into.
 *
 * data()/size() returns the key.
 * nameSpacedData()/nameSpacedSize() returns the DocNamespace prefixed buffer.
 */
class StoredDocKey : public DocKey {
public:

    /*
     * Create a StoredDocKey from key to key+nkey in docNamespace.
     */
    StoredDocKey(const uint8_t* key, size_t nkey, DocNamespace docNamespace)
        : DocKey(nullptr, nkey, docNamespace),
          storage(new uint8_t[nkey + terminatorBytes + namespaceBytes]()) {
        storage[0] = static_cast<uint8_t>(docNamespace);

        // Set buf to the first byte of storage
        buf = &storage[1];
        // copy key into buf[1] (but const so go via storage)
        std::copy(key, key + nkey, &storage[1]);
    }

    /*
     * Create a StoredDocKey from a DocKey
     */
    StoredDocKey(const DocKey key)
        : StoredDocKey(key.data(), key.size(), key.doc_namespace) {
    }

    /*
     * Create a StoredDocKey from a zero terminated C-string
     */
    StoredDocKey(const char* cString, DocNamespace docNamespace)
        : StoredDocKey(reinterpret_cast<const uint8_t*>(cString),
                     std::strlen(cString), docNamespace) {
    }

    /*
     * Create a StoredDocKey from a std::string
     */
    StoredDocKey(const std::string& key, DocNamespace docNamespace)
        : StoredDocKey(reinterpret_cast<const uint8_t*>(key.c_str()),
                     key.size(), docNamespace) {
    }

    /*
     * Create a StoredDocKey from a buffer that was orginally obtained from
     * nameSpacedData()
     */
    StoredDocKey(const uint8_t* key, size_t nkey)
        : DocKey(nullptr, nkey - namespaceBytes, DocNamespace::DefaultCollection),
          storage(new uint8_t[nkey + terminatorBytes]()) {
        std::copy(key, key + nkey, storage.get());
        // Set buf so that data() returns the key
        buf = &storage[1];
        // Set doc_namespace from the first byte of the input data
        doc_namespace = static_cast<DocNamespace>(storage[0]);
    }

    StoredDocKey(const StoredDocKey& other)
        : DocKey(nullptr, other.size(), other.getDocNamespace()),
          storage(new uint8_t[other.size() + terminatorBytes + namespaceBytes]()) {
        buf = &storage[1];
        storage[0] = static_cast<uint8_t>(doc_namespace);;
    }

    StoredDocKey& operator= (StoredDocKey other) {
        std::swap(len, other.len);
        std::swap(buf, other.buf);
        std::swap(doc_namespace, other.doc_namespace);
        std::swap(storage, other.storage);
        return *this;
    }

    /*
     * Create a StoredDocKey from its serialised equivalent.
     */
    StoredDocKey(const SerialisedDocKey& key);

    const uint8_t* data() const {
        return buf;
    }

    size_t size() const {
        return len - terminatorBytes - namespaceBytes;
    }

    DocNamespace getDocNamespace() const {
        return doc_namespace;
    }

    const uint8_t* nameSpacedData() const {
        return storage.get();
    }

    size_t nameSpacedSize() const {
        return len - terminatorBytes;
    }

    bool operator == (const StoredDocKey& rhs) const {
        return nameSpacedSize() == rhs.nameSpacedSize() &&
            std::memcmp(nameSpacedData(), rhs.nameSpacedData(), nameSpacedSize()) == 0;
    }

    bool operator != (const StoredDocKey& rhs) const {
        return !(*this == rhs);
    }

    bool operator < (const StoredDocKey& rhs) const {
        return std::lexicographical_compare(nameSpacedData(),
                                            nameSpacedData() + nameSpacedSize(),
                                            rhs.nameSpacedData(),
                                            rhs.nameSpacedData() + rhs.nameSpacedSize());
    }

protected:

    static const size_t terminatorBytes = 1;
    static const size_t namespaceBytes = sizeof(std::underlying_type<DocNamespace>::type);
    std::unique_ptr<uint8_t[]> storage;
};


class MutationLogEntry;
class StoredValue;

/*
 * SerialisedDocKey maintains the key in a single continuous allocation.
 * For example where the StoredDocKey data needs to exist as part of a bigger
 * contiguous block of data for writing to disk.
 *
 * A limited number of classes are friends and can directly construct this key.
 *  - A SerialisedDocKey is not C-string safe
 */
class SerialisedDocKey {
public:
    /*
     * The copy constructor is deleted.
     * Copying a SerialisedDocKey is dangerous due to the bytes living
     * outside of the object.
     */
    SerialisedDocKey(const SerialisedDocKey &obj) = delete;

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

    const DocKey getDocKey() const {
        return DocKey(data(), size(), docNamespace);
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
        return sizeof(SerialisedDocKey) + len - 1;
    }

    /*
     * Allocate the correct storage for nkey bytes and return the
     * SerialisedDocKey as a unique_ptr
     */
    static std::unique_ptr<SerialisedDocKey> make(const uint8_t* key,
                                                      size_t nkey,
                                                      DocNamespace docNamespace) {
        std::unique_ptr<SerialisedDocKey>
            rval(reinterpret_cast<SerialisedDocKey*>(new uint8_t[getObjectSize(nkey)]));
        new (rval.get()) SerialisedDocKey(key, nkey, docNamespace);
        return rval;
    }

    static std::unique_ptr<SerialisedDocKey> make(const StoredDocKey& key) {
        return make(key.data(), key.size(), key.getDocNamespace());
    }

    /*
     * Get a hash of the SerialisedDocKey
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

    SerialisedDocKey() : length(0) {}

    /*
     * Copy-in c to c+n bytes.
     * n must be less than 255
     */
    SerialisedDocKey(const uint8_t* c, uint8_t n, DocNamespace docNamespace) {
        if (n > std::numeric_limits<uint8_t>::max()) {
            throw std::length_error("SerialisedDocKey size exceeded " +
                                    std::to_string(n));
        }
        length = n;
        std::memcpy(bytes, c, n);
        this->docNamespace = docNamespace;
    }

    /*
     * SerialisedDocKey can be created from a StoredDocKey.
     */
    SerialisedDocKey(const StoredDocKey& key)       : length(key.size()){
    docNamespace = key.doc_namespace;
    std::memcpy(bytes, key.data(), key.size());
}

    uint8_t length;
    DocNamespace docNamespace;
    uint8_t bytes[1]; // key bytes
};

inline StoredDocKey::StoredDocKey(const SerialisedDocKey& key)
      : StoredDocKey(DocKey(key.data(), key.size(), key.getDocNamespace())) {}



/*
 * A hash function for StoredDocKey so they can be used in std::map and friends.
 */
namespace std {
    template<>
    struct hash<StoredDocKey> {
        std::size_t operator()(const StoredDocKey& s) const {
            return hashDocKey(s);
        }
    };
}
