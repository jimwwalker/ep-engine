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
#include <unordered_set>


namespace Collections {

class Manifest {
public:

    /*
     * Initialise the default manifest
     */
    Manifest();

    /*
     * Create a manifest from json input as per SET_COLLECTIONS command
     * throws invalid_argument if the json cannot be parsed or contains
     * illegal stuff.
     */
    Manifest(const char* json, size_t jsonLen);

    bool isKeyAllowed();

    // TODO create a manifest from DCP input, as if we learned about
    // the collection first via DCP
    // DCP could tell a replica *before* ns_server tells us

    int getRevision() const {
        return revision;
    }

    const char* getSeparator() const {
        return separator.c_str();
    }

private:

    void validateAndChangeSeparator(const char* newSeparator);
    static bool validSeparator(const char* separator);

    int revision;
    std::string separator;
    std::unordered_set<std::string> collections;

    /*
     * When a manifest contains $default when a key doesn't match
     * a collection it is allowed.
     */
    bool containsDefaultCollection;

    static const std::string defaultCollection;
};

};
