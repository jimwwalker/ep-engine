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

#include "ep_types.h"
#include "storagekey.h"

/*
 * Create a StorageKey object from a C string in the default collection.
 */
inline StorageKey makeStorageKey(const char* c_string) {
    return StorageKey(c_string,
                      std::strlen(c_string) + 1,
                      StorageMetaFlag::DefaultCollection);
}

/*
 * Create a StorageKey object from a std::string in the default collection.
 */
inline StorageKey makeStorageKey(const std::string& string) {
    return makeStorageKey(string.c_str());
}