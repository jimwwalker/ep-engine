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

#include <vector>

#include "ep_types.h"

#include <platform/crc32c.h>

class ProtocolKey;
class SerialisedStorageKey;
class SerialisedProtocolKey;

/*
 * StorageKey is a container used to represent a key inside our storage
 * mediums.
 */
class StorageKey {
    /*
     * StorageKeyAllocator is a simple class to manage how StorageKey uses the
     * heap.
     *
     * The majority of StorageKey use-cases will require storage to be heap
     * allocated and key data copied into that heap allocation.
     *
     * StorageKeyAllocator though allows existing key data to be used.
     * This is for places that don't justify heap allocation and copying
     * (perhaps due to performance requirements). The StorageKeyAllocator can
     * frame that data for the StorageKey to use directly with no heap
     * allocation/copying/deallocation required.
     */
    template <class T>
    class StorageKeyAllocator {
    public:
        typedef T value_type;
        StorageKeyAllocator() : existingKey(nullptr), length(0) {}
        StorageKeyAllocator(T* n, size_t len) : existingKey(n), length(len) {}

        /*
         * Return existingKey only if it's not a nullptr and length matches n
         */
        T* allocate(std::size_t n) {
            if (existingKey && n == length) {
                return existingKey;
            } else {
                return new T[n];
            }
        }

        /*
         * Free p when it is not a match for the existingKey
         */
        void deallocate(T* p, std::size_t n) {
            if (p != existingKey) {
                delete [] p;
            }
        }

        /*
         * overload construct so that we can intialise when the element v is
         * outside of the existingKey allocation, but when v is inside the
         * existingKey allocation, it remains un-touched.
         */
        template <class U, class... Args> void construct(U* v, Args&&...args) {
            if (v < existingKey ||
                v >= (static_cast<char*>(existingKey)+length)) {
                new((void *)v) U(std::forward<Args>(args)...);
            }
        }

        /*
         * Return 1 or 0 bytes.
         * We can only guarantee terminating bytes if the StorageKey is heap
         * allocated and existingKey is nullptr.
         */
        size_t getTerminatingBytes() {
            return size_t(existingKey == nullptr);
        }

        bool operator==(const StorageKeyAllocator<T>&b) {
            // Memory from one StorageKeyAllocator can be freed by another.
            return true;
        }

        bool operator!=(const StorageKeyAllocator<T>& rhs) {
            return !(*this==rhs);
        }
    private:
        /*
         * The Allocator can be constructed to use existing memory and skip heap
         * allocation. This is the location and size of the existing location.
         */
        T* existingKey;
        size_t length;
    };

public:

    /*
     * A StorageKey can be created from a char* c, length n and a meta flag.
     * Triggers heap allocation and a copy-in of the data.
     */
    StorageKey(const char* c, size_t n, StorageMetaFlag flag)
          : storage(n + metaBytes + 1) {
        storage[0] = static_cast<std::underlying_type<StorageMetaFlag>::type>(flag);
        std::copy(c, c + n, storage.data() + metaBytes);
        storage.back() = 0;
    }

    /*
     * StorageKey can be created from a std::string
     */
    StorageKey(const std::string& s, StorageMetaFlag flag)
          : StorageKey(s.data(), s.size()) {}

    /*
     * A StorageKey can be created from a char* c and length n.
     * The c/n buffer is assmed to contain the StorageMetaFlag
     */
    StorageKey(const char* c, size_t n)
          : storage(n + 1) {
        std::copy(c, c + n, storage.data());
        storage.back() = 0;
    }

    /*
     * A StorageKey can be created from its serialised equivalent.
     */
    StorageKey(const SerialisedStorageKey& key);

    const ProtocolKey& getProtocolKey() const;

    const char* data() const {
        return storage.data();
    }

    size_t size() const {
        return storage.size() - storage.get_allocator().getTerminatingBytes();
    }

    StorageMetaFlag getMetaFlag() const {
        return static_cast<StorageMetaFlag>(storage.at(0));
    }

    bool operator == (const StorageKey& rhs) const {
        return storage == rhs.storage;
    }

    bool operator != (const StorageKey& rhs) const {
        return storage != rhs.storage;
    }

    bool operator < (const StorageKey& rhs) const {
        return storage < rhs.storage;
    }

    bool operator > (const StorageKey& rhs) const {
        return storage > rhs.storage;
    }

    bool operator <= (const StorageKey& rhs) const {
        return storage <= rhs.storage;
    }

    bool operator >= (const StorageKey& rhs) const {
        return storage >= rhs.storage;
    }

protected:
    const char* getStartOfProtocolKey() const {
        return storage.data() + metaBytes;
    }

    size_t getSizeofProtocolKey() const {
        return size() - metaBytes;
    }

    /*
     * Finally a StorageKey can be created that doesn't heap allocate.
     */
    struct NoHeapAllocation{};
    StorageKey(const char* c, size_t n, NoHeapAllocation)
            // have to remove const so that the allocator::allocate method
            // can retrurn the pointer as non const.
          : storage(StorageKeyAllocator<char>(const_cast<char*>(c), n)) {
            storage.resize(n);
    }

private:
    static const int metaBytes = sizeof(std::underlying_type<StorageMetaFlag>::type);
    std::vector<char, StorageKeyAllocator<char> > storage;
};

class ProtocolKey : public StorageKey {
public:
    /*
     * ProtocolKey currently deleted from being constructed standalone.
     * The hierarchy does allow this though if needed.
     */
    ProtocolKey() = delete;

    const char* data() const {
        return getStartOfProtocolKey();
    }

    size_t size() const {
        return getSizeofProtocolKey();
    }
};

/*
 * A special StorageKey that doesn't heap allocate, it will use the memory
 * c to c+(n-1) and comes with caveats:
 *  - Modifictions to c to c+(n-1) will be seen through data()
 *  - data() cannot be guaranteed to be C-string safe.
 */
class StorageKeyNoHeap : public StorageKey {
public:
    StorageKeyNoHeap(const char* c, size_t n)
          : StorageKey(c, n, StorageKey::NoHeapAllocation{}) {}
};

inline const ProtocolKey& StorageKey::getProtocolKey() const {
    return static_cast<const ProtocolKey&>(*this);
}

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

    const char* data() const {
        return bytes;
    }

    size_t size() const {
        return length;
    }

    StorageMetaFlag getMetaFlag() const {
        return static_cast<StorageMetaFlag>(bytes[0]);
    }

    /*
     * Return how many bytes are (or should be) allocated to this object
     */
    size_t getObjectSize() const {
        return getObjectSize(length);
    }

    const SerialisedProtocolKey& getProtocolKey() const;

    /*
     * Return how many bytes are needed to store a key of len.
     */
    static size_t getObjectSize(size_t len) {
        return sizeof(SerialisedStorageKey) + (metaBytes + len) - 1;
    }

    static std::unique_ptr<SerialisedStorageKey> make(const char* c, size_t n, StorageMetaFlag flag) {
        std::unique_ptr<SerialisedStorageKey> rval(reinterpret_cast<SerialisedStorageKey*>(new char[getObjectSize(n)]));
        new (rval.get()) SerialisedStorageKey(c, n, flag);
        return rval;
   }

protected:
    const char* getStartOfProtocolKey() const {
        return &bytes[metaBytes];
    }

    size_t getSizeofProtocolKey() const {
        return size() - metaBytes;
    }

private:

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
    SerialisedStorageKey(const char* c, uint8_t n, StorageMetaFlag flag) {
        if ((n + metaBytes) > std::numeric_limits<uint8_t>::max()) {
            throw std::length_error("SerialisedStorageKey size exceeded " +
                                    std::to_string(n + metaBytes));
        }
        length = n + metaBytes;
        bytes[0] = static_cast<std::underlying_type<StorageMetaFlag>::type>(flag);
        std::memcpy(&bytes[metaBytes], c, n);
    }

    /*
     * SerialisedStorageKey can be created from a StorageKey.
     */
    SerialisedStorageKey(const StorageKey& key);

    static const int metaBytes = sizeof(std::underlying_type<StorageMetaFlag>::type);
    uint8_t length;
    char bytes[1];
};

class SerialisedProtocolKey : public SerialisedStorageKey {
public:
    SerialisedProtocolKey() = delete;

    const char* data() const {
        return getStartOfProtocolKey();
    }

    size_t size() const {
        return getSizeofProtocolKey();
    }
};

inline StorageKey::StorageKey(const SerialisedStorageKey& key)
      : StorageKey(key.data(), key.size()) {}

inline SerialisedStorageKey::SerialisedStorageKey(const StorageKey& key)
      : length(key.size()){
    std::memcpy(bytes, key.data(), key.size());
}

inline const SerialisedProtocolKey& SerialisedStorageKey::getProtocolKey() const {
    return static_cast<const SerialisedProtocolKey&>(*this);
}

/*
 * A hash function for StorageKey so they can be used in std::map and friends.
 */
namespace std {
    template<>
    struct hash<StorageKey> {
        std::size_t operator()(const StorageKey& s) const {
            return crc32c(reinterpret_cast<const uint8_t*>(s.data()),
                          s.size(),
                          0);
        }
    };
}
