/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <cstdint>
#include <string>
#include <vector>

namespace Collections {

/**
 * Manifest is an object that is constructed from JSON data as per
 * a set_collections command
 *
 * Users of this class can then obtain the revision, separator and
 * all collections that are included in the manifest.
 */
class Manifest {
public:
    /*
     * Initialise the default manifest
     */
    Manifest();

    /*
     * Create a manifest from json.
     * Validates the json as per SET_COLLECTIONS rules.
     */
    Manifest(const std::string& json);

    int getRevision() const {
        return revision;
    }

    const std::string& getSeparator() const {
        return separator;
    }

    bool isDefaultCollectionEnabled() const {
        return containsDefaultCollection;
    }

    std::vector<std::string>::iterator begin() {
        return collections.begin();
    }

    std::vector<std::string>::iterator end() {
        return collections.end();
    }

    size_t size() const {
        return collections.size();
    }

    std::vector<std::string>::iterator find(const std::string& collection) {
        for (auto itr = begin(); itr != end(); itr++) {
            if (collection == *itr) {
                return itr;
            }
        }
        return end();
    }

private:
    static bool validSeparator(const char* separator);

    int revision;
    std::string separator;
    std::vector<std::string> collections;

    /*
     * When a manifest contains $default when a key doesn't match
     * a collection it is allowed.
     */
    bool containsDefaultCollection;

    static const std::string defaultCollection;
};

} // end namespace Collections
