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

#include <string>

#include "item.h"

/// underlying size of uint32_t as this is to be stored in the Item flags field.
enum class SystemEvent : uint32_t {
    CreateCollection,
    BeginDeleteCollection,
    DeleteCollectionHard,
    DeleteCollectionSoft
};

static inline std::string to_string(const SystemEvent se) {
    switch (se) {
    case SystemEvent::CreateCollection:
        return "CreateCollection";
    case SystemEvent::BeginDeleteCollection:
        return "BeginDeleteCollection";
    case SystemEvent::DeleteCollectionHard:
        return "DeleteCollectionHard";
    case SystemEvent::DeleteCollectionSoft:
        return "DeleteCollectionSoft";
    default:
        throw std::invalid_argument("to_string(SystemEvent) unknown " +
                                    std::to_string(int(se)));
        return "";
    }
}

class SystemEventFactory {
public:
    /**
     * Make an Item representing the SystemEvent
     * @param se The SystemEvent being created. The returned Item will have this
     *           value stored in the flags field.
     * @param keyExtra Every SystemEvent has defined key, keyExtra is appended
     *        to the defined key
     * @param itemSize The returned Item can be requested to allocate a value
     *        of itemSize. Some SystemEvents will update the value with data to
     *        be persisted/replicated.
     */
    static std::unique_ptr<Item> make(SystemEvent se,
                                      const std::string& keyExtra,
                                      size_t itemSize);
};

class KVStore;

enum class SystemEventFlushStatus { Skip, Continue };

/**
 * SystemEventFlush holds all SystemEvent data for a single invocation of a
 * vbucket's flush
 * If the flush encountered no SystemEvents then this class does nothing
 * If the flush has SystemEvents then this class will ensure the correct
 * actions occur.
 */
class SystemEventFlush {
public:
    SystemEventFlush(KVStore& kvs, uint16_t vb) : vbid(vb), kvstore(kvs) {
    }

    /**
     * The flusher calls this after all items have been flushed, passing
     * how many items were flushed.
     *
     * if 0 items were flushed, then this function may need to request a full
     * update of meta-data documents.
     */
    void commitIfNeeded(int itemsFlushed);

    /**
     * The flusher passes each item into this function and process determines
     * what needs to happen (possibly updating the Item)
     *
     * @param item an item from the flushers items to flush.
     * @returns Skip if the flusher should not continue with the item or
     *          Continue if the flusher can continue the rest of the flushing
     *          function against the item.
     */
    SystemEventFlushStatus process(const queued_item& item);

private:
    void maybeUpdateKVStoreCollectionsManifest(const queued_item& item);

    uint16_t vbid;
    KVStore& kvstore;
    queued_item collectionManifestItem;
};
