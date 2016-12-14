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

#include "collections/vbucket_manifest.h"
#include "collections/manifest.h"

#include <platform/make_unique.h>

void Collections::VBucket::Manifest::addCollection(
        const std::string& collection, int revision, int64_t seqno) {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        auto m = std::make_unique<Collections::VBucket::ManifestEntry>(
                collection, revision, seqno);
        map.emplace(m->getCharBuffer(), std::move(m));
    } else {
        // Update
        if (itr->second->isDeleting() && seqno > itr->second->getStartSeqno()) {
            itr->second->setRevision(revision);
            itr->second->setStartSeqno(seqno);
        } else {
            std::stringstream manifestState;
            manifestState << *itr->second;
            throw std::logic_error(
                    "VBucket::Manifest::addCollection - failing."
                    " seqno: " +
                    std::to_string(seqno) + " found entry " +
                    manifestState.str());
        }
    }
}

void Collections::VBucket::Manifest::processManifest(
        Collections::Manifest manifest,
        std::vector<std::string>& additions,
        std::vector<std::string>& deletions) const {
    for (auto& manifestEntry : map) {
        // Does manifestEntry::collectionName exist in manifest? NO - delete
        // time
        if (manifest.find(manifestEntry.second->getCollectionName()) ==
            manifest.end()) {
            deletions.push_back(std::string(
                    reinterpret_cast<const char*>(manifestEntry.first.data()),
                    manifestEntry.first.size()));
        }
    }

    // iterate manifest and add all non-existent collection
    for (auto m : manifest) {
        // if we don't find the collection, then it must be an addition.
        // if we do find a name match, then check if the collection is in the
        //  process of being deleted.
        auto itr = map.find({m.data(), m.size()});

        if (itr == map.end() || itr->second->isDeleting()) {
            additions.push_back(m);
        }
    }
}

std::ostream& Collections::VBucket::operator<<(
        std::ostream& os, const Collections::VBucket::Manifest& manifest) {
    os << "VBucket::Manifest: size:" << manifest.map.size() << std::endl;
    for (auto& m : manifest.map) {
        os << *m.second << std::endl;
    }
    return os;
}