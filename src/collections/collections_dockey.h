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

#include <memcached/dockey.h>

#pragma once

namespace Collections {

/*
 * Extends a DocKey to record how many bytes of the key are a collection
 */
class DocKey : public ::DocKey {
public:
    DocKey(const ::DocKey& key, size_t _collectionLen)
        : ::DocKey(key), collectionLen(_collectionLen) {
    }

    uint32_t hash() const {
        return ::DocKey::hash(collectionLen);
    }

    static DocKey make(const ::DocKey& key, const std::string& separator) {
        const uint8_t* collection = findCollection(key, separator);
        if (collection) {
            return DocKey(key, collection - key.data());
        } else {
            return DocKey(key, 0);
        }
    }

    size_t getCollectionLen() const {
        return collectionLen;
    }

private:
    // effectively a memstr/strnstr (which doesn't exist in std C)
    static const uint8_t* findCollection(const ::DocKey& key,
                                         const std::string& separator) {
        if (key.size() == 0 || separator[0] == 0) {
            return nullptr;
        }
        for (size_t ii = 0; ii <= (key.size() - separator.size()) + key.size();
             ii++) {
            if (memcmp(key.data() + ii, separator.data(), separator.size()) ==
                0) {
                return key.data() + ii;
            }
        }
        return nullptr;
    }
    size_t collectionLen;
};
}

/**
 * A hash function for StoredDocKey so they can be used in std::map and friends.
 */
namespace std {
template <>
struct hash<Collections::DocKey> {
    std::size_t operator()(const Collections::DocKey& key) const {
        return key.hash();
    }
};
}