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

#include "stored-value.h"

#include <platform/sized_buffer.h>

#include <memory>

namespace Collections {
namespace VBucket {

class ManifestEntry {
public:
    ManifestEntry(const cb::const_char_buffer& name,
                  int rev,
                  int64_t _start_seqno)
        : collectionName(new std::string(name.data(), name.size())),
          revision(rev),
          start_seqno(_start_seqno),
          end_seqno(StoredValue::state_collection_open) {
    }

    ManifestEntry& operator=(ManifestEntry&& rhs) {
        collectionName = std::move(rhs.collectionName);
        revision = rhs.revision;
        start_seqno = rhs.start_seqno;
        end_seqno = rhs.end_seqno;
        return *this;
    }

    ManifestEntry& operator=(ManifestEntry rhs) {
        std::swap(collectionName, rhs.collectionName);
        revision = rhs.revision;
        start_seqno = rhs.start_seqno;
        end_seqno = rhs.end_seqno;
        return *this;
    }

    ManifestEntry(const ManifestEntry& rhs) {
        collectionName.reset(new std::string(rhs.collectionName->c_str()));
        revision = rhs.revision;
        start_seqno = rhs.start_seqno;
        end_seqno = rhs.end_seqno;
    }

    const std::string& getCollectionName() const {
        return *collectionName;
    }

    cb::const_char_buffer getCharBuffer() const {
        return cb::const_char_buffer(collectionName->data(),
                                     collectionName->size());
    }

    int64_t getStartSeqno() const {
        return start_seqno;
    }

    void setStartSeqno(int64_t seqno) {
        start_seqno = seqno;
    }

    int64_t getEndSeqno() const {
        return end_seqno;
    }

    void setEndSeqno(int64_t seqno) {
        end_seqno = seqno;
    }

    void resetEndSeqno() {
        end_seqno = StoredValue::state_collection_open;
    }

    void setRevision(int rev) {
        revision = rev;
    }

    bool isDeleting() const {
        return end_seqno > start_seqno;
    }

    friend std::ostream& operator<<(std::ostream& os,
                                    const ManifestEntry& manifestEntry);

private:
    std::unique_ptr<std::string> collectionName;

    int revision; // the Manifest revision we first saw this in
    // collection lifetime start to end
    int64_t start_seqno;
    int64_t end_seqno;
};
} // end namespace VBucket
} // end namespace Collections