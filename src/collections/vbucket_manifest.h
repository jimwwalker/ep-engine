/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "rwlock.h"
#include "systemevent.h"

#include <platform/sized_buffer.h>

#include <mutex>
#include <unordered_map>

class VBucket;

namespace Collections {
namespace VB {

/**
 * Collections::VB::Manifest is a container for all of the collections a VBucket
 * knows about.
 *
 * Each collection is represented by a Collections::VB::ManifestEntry and all of
 * the collections are stored in an unordered_map. The map is implemented to
 * allow look-up by collection-name without having to allocate a string as the
 * data-path will need to do collection validity checks with minimal penalty.
 *
 * The Manifest allows for an external manager to drive the lifetime of each
 * collection - adding, begin/complete of the deletion phase.
 *
 * This class is intended to be thread safe - the useage pattern is likely that
 * we will have many threads reading the collection state (i.e. policing
 * incoming database operations) whilst occasionally the collection state will
 * be changed by another thread calling "update" or "completeDeletion".
 *
 */
class Manifest {
public:
    /**
     * Map from a 'string_view' to an entry.
     * The key points to data stored in the value (which is actually a pointer
     * to a value to remove issues where objects move).
     * Using the string_view as the key allows faster lookups, the caller
     * need not heap allocate.
     */
    using container = ::std::unordered_map<cb::const_char_buffer,
                                           std::unique_ptr<ManifestEntry>>;

    /**
     * Construct a VBucket::Manifest from a JSON string or an empty string.
     *
     * Empty string allows for construction where no JSON data was found i.e.
     * an upgrade occured and this is the first construction of a manifest
     * for a VBucket which has persisted data, but no manifest data. When an
     * empty string is used, the manifest will initialise with default settings.
     * - Default Collection enabled.
     * - Separator defined as Collections::DefaultSeparator
     *
     * A non-empty string must be a valid JSON manifest that determines which
     * collections to instantiate.
     *
     * @param manifest An empty string or valid JSON data to construct the
     *                 object from.
     */
    Manifest(const std::string& manifest);

    /**
     * Update from a Collections::Manifest
     *
     * Update compares the current collection set against the manifest and
     * triggers collection creation and collection deletion.
     *
     * Creation and deletion of a collection are pushed into the VBucket and
     * the seqno of updates is recorded in the manifest.
     *
     * @param vb The VBucket to update (queue data into).
     * @param manifest The incoming manifest to compare this object with.
     */
    void update(::VBucket& vb, const Collections::Manifest& manifest);

    /**
     * Complete the deletion of a collection.
     *
     * Lookup the collection name and determine the deletion actions.
     * A collection could of been added again during a background delete so
     * completeDeletion may just update the state or fully drop all knowledge of
     * the collection.
     *
     * @param vb The VBucket in which the deletion is occuring.
     * @param collection The collection that is being deleted.
     * @param revision The Manifest revision which initiated the delete.
     */
    void completeDeletion(::VBucket& vb,
                          const std::string& collection,
                          uint32_t revision);

    /**
     * Does the key contain a valid collection?
     *
     * - If the key applies to the default collection, the default collection
     *   must exist.
     *
     * - If the key applies to a collection, the collection must exist and must
     *   not be in the process of deletion.
     */
    bool doesKeyContainValidCollection(const ::DocKey& key) const;

    /**
     * Return a std::string containing a JSON representation of a
     * VBucket::Manifest. The input data should be a previously serialised
     * object, i.e. the input to this function is the output of
     * populateWithSerialisedData.
     *
     * The function also corrects the seqno of the entry which initiated a
     * manifest update (i.e. a collection create or delete). This is because at
     * the time of serialisation, the collection SystemEvent Item did not have a
     * seqno.
     *
     * @param se The SystemEvent triggering the JSON generation.
     * @param buffer The raw data to process.
     * @param finalEntrySeqno The correct seqno to use for the final entry of
     *        the serialised data.
     */
    static std::string serialToJson(SystemEvent se,
                                    cb::const_char_buffer buffer,
                                    int64_t finalEntrySeqno);

protected:
    /**
     * Add a collection to the manifest specifing the Collections::Manifest
     * revision that it was seen in and the sequence number for the point in
     * 'time' it was created.
     *
     * @param collection Name of the collection to add.
     * @param revision The revision of the Collections::Manifest triggering this
     *        add.
     * @param seqno The seqno of an Item which represents the creation event.
     */
    void addCollection(const std::string& collection,
                       uint32_t revision,
                       int64_t startSeqno,
                       int64_t endSeqno);

    /**
     * Begin the deletion process by marking the collection with the seqno that
     * represents its end.
     *
     * After "begin" delete a collection can be added again or fully deleted
     * by the completeDeletion method.
     *
     * @param collection Name of the collection to delete.
     * @param revision Revision of the Manifest triggering the delete.
     * @param seqno The seqno of the deleted event mutation for the collection
     *        deletion,
     */
    void beginDelCollection(const std::string& collection,
                            uint32_t revision,
                            int64_t seqno);

    /**
     * Processs a Collections::Manifest
     *
     * This function returns two sets of collections. Those which are being
     * added and those which are being deleted.
     *
     * @param manifest The Manifest to compare with.
     * @returns A pair of vector containing the required changes, first contains
     *          collections that need adding whilst second contains those which
     *          should be deleted.
     */
    using processResult =
            std::pair<std::vector<std::string>, std::vector<std::string>>;
    processResult processManifest(const Collections::Manifest& manifest) const;

    /**
     * Create a SystemEvent Item, the Item's value will contain data for later
     * consumption by serialToJson
     *
     * @param se SystemEvent to create.
     * @param collection The collection which is changing,
     * @param revision Manifest revision triggering the update.
     */
    std::unique_ptr<Item> createSystemEvent(SystemEvent se,
                                            const std::string& collection,
                                            uint32_t revision) const;

    /**
     * Create an Item that carries a system event and queue it.
     * @param vb The Vbucket to update.
     * @param se The SystemEvent to create and queue.
     */
    int64_t queueSystemEvent(::VBucket& vb,
                             SystemEvent se,
                             const std::string& collection,
                             uint32_t revision) const;

    /**
     * Obtain how many bytes of storage are needed for a serialised copy
     * of this object including if required, the size of the changing
     * collection.
     * @param collection The name of the collection being changed. It's size is
     *        included in the returned value.
     */
    size_t getSerialisedDataSize(const std::string& collection) const;

    /**
     * Populate a buffer with the serialised state of the manifest and one
     * additional entry that is the collection being changed, i.e. the addition
     * or deletion.
     *
     * @param out The location and size we wish to update
     * @param revision The Manifest revision we are processing
     * @param collection The collection being added/deleted
     * @param se The SystemEvent we're working on
     */
    void populateWithSerialisedData(cb::char_buffer out,
                                    const std::string& collection,
                                    uint32_t revision,
                                    SystemEvent se) const;

    /**
     * Return the string for the key
     */
    const char* getJsonEntry(cJSON* cJson, const char* key);

    /**
     * The current set of collections
     */
    container map;

    /**
     * Does the current set contain the default collection?
     */
    bool defaultCollectionExists;

    /**
     * The collection separator
     */
    const std::string separator;

    /**
     * shared lock to allow concurrent readers and safe updates
     */
    mutable RWLock lock;

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);
};

std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

} // end namespace VB
} // end namespace Collections
