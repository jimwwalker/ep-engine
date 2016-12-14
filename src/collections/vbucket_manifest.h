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

#pragma once

#include "collections/collections_dockey.h"
#include "collections/collections_types.h"
#include "collections/manifest.h"
#include "collections/vbucket_manifest_entry.h"

#include <unordered_map>

namespace Collections {
namespace VBucket {

/**
 * Manifest represents a vbucket's view of collections
 */
    // TODO $default handling, i.e. knowing it's gone
class Manifest {
public:
    using container = ::std::unordered_map<cb::const_char_buffer,
                                           std::unique_ptr<ManifestEntry>>;

    /*
        by default a Manifest always contains default until a set_collections
        positively removes it? but Manifest should intially be formed from
        _local document cache which for a new VB is empty so 2 constructors
        needed one for warm VB (init from locastat) one for cold/new
    */
    Manifest() {
        //  Manifest is empty for brand new VB
    }

    // 2 something like this, read initial state loaded from VB file on disk
    Manifest(const std::string persistedState) {
        // TODO, maybe this constructor takes an empty string for new VB?
    }

    void addCollection(const std::string& collection,
                       int revision,
                       int64_t seqno);

    /**
     * Deletion begins by just updating the end seqno with the seqno of the
     * deletion event meta-mutation.
     */
    void beginDelCollection(const std::string& collection,
                            int64_t seqno) const {
        auto itr = map.find({collection.data(), collection.size()});
        if (itr != map.end()) {
            itr->second->setEndSeqno(seqno);
        }
    }

    void completeDelCollection(const std::string& collection) {
        auto itr = map.find({collection.data(), collection.size()});
        if (itr != map.end() && itr->second->isDeleting()) {
            map.erase(collection);
        } else {
            // else.. it is ok to skip deletion, the assumption is that between
            // begin/complete the collection was added again.
            // In this case, reset the endseqno to be the 'special' open value
            itr->second->resetEndSeqno();
        }
    }

    /**
     * Process a Collections::Manifest against this
     * Collections::VBucket::Manifest
     * This function computes which collections are being added and which
     * are being deleted.
     */
    void processManifest(Collections::Manifest manifest,
                         std::vector<std::string>& additions,
                         std::vector<std::string>& deletions) const;

    bool collectionExists(cb::const_char_buffer collection) const {
        auto itr = map.find(collection);
        // todo: check start_seq > end ? a deleted/deleting collection...
        return itr != map.end();
    }

    bool collectionExists(const Collections::DocKey& key) const {
        return collectionExists({reinterpret_cast<const char*>(key.data()),
                                 key.getCollectionLen()});
    }

    container::iterator begin() {
        return map.begin();
    }

    container::iterator end() {
        return map.end();
    }

    size_t size() const {
        return map.size();
    }

protected:
    container::iterator find(const std::string& collection) {
        return map.find({collection.data(), collection.size()});
    }

    /**
     * map from a "string view" to the ManifestEntry
     * The key is pointing to data owned by the value and allows for the
     * find/exists logic to look up collections without having to allocate
     */
    container map;

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);
};
} // end namespace VBucket
} // end namespace Collections
