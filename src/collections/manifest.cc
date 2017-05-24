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

#include "collections/manifest.h"
#include "collections/collections_types.h"

#include <JSON_checker.h>
#include <cJSON.h>
#include <cJSON_utils.h>

#include <cstring>
#include <iostream>

namespace Collections {

Manifest::Manifest()
    : revision(0), defaultCollectionExists(true), separator(DefaultSeparator) {
    collections.push_back(DefaultCollectionIdentifier.data());
}

Manifest::Manifest(const std::string& json)
    : revision(-1), defaultCollectionExists(false) {
    if (!checkUTF8JSON(reinterpret_cast<const unsigned char*>(json.data()),
                       json.size())) {
        throw std::invalid_argument("Manifest::Manifest input not valid json");
    }

    unique_cJSON_ptr cjson(cJSON_Parse(json.c_str()));
    if (!cjson) {
        throw std::invalid_argument(
                "Manifest::Manifest cJSON cannot parse json");
    }

    auto jsonRevision = cJSON_GetObjectItem(cjson.get(), "revision");
    if (!jsonRevision || jsonRevision->type != cJSON_Number) {
        throw std::invalid_argument(
                "Manifest::Manifest cannot find valid "
                "revision: " +
                (!jsonRevision ? "nullptr"
                               : std::to_string(jsonRevision->type)));
    } else {
        revision = jsonRevision->valueint;
    }

    auto jsonSeparator = cJSON_GetObjectItem(cjson.get(), "separator");
    if (!jsonSeparator || jsonSeparator->type != cJSON_String) {
        throw std::invalid_argument(
                "Manifest::Manifest cannot find valid "
                "separator: " +
                (!jsonSeparator ? "nullptr"
                                : std::to_string(jsonSeparator->type)));
    } else if (validSeparator(jsonSeparator->valuestring)) {
        separator = jsonSeparator->valuestring;
    } else {
        throw std::invalid_argument("Manifest::Manifest separator invalid " +
                                    std::string(jsonSeparator->valuestring));
    }

    auto jsonCollections = cJSON_GetObjectItem(cjson.get(), "collections");
    if (!jsonCollections || jsonCollections->type != cJSON_Array) {
        throw std::invalid_argument(
                "Manifest::Manifest cannot find valid "
                "collections: " +
                (!jsonCollections ? "nullptr"
                                  : std::to_string(jsonCollections->type)));
    } else {
        for (int ii = 0; ii < cJSON_GetArraySize(jsonCollections); ii++) {
            auto collection = cJSON_GetArrayItem(jsonCollections, ii);
            if (!collection || collection->type != cJSON_String) {
                throw std::invalid_argument(
                        "Manifest::Manifest cannot find "
                        "valid collection for index " +
                        std::to_string(ii) +
                        (!collection ? " nullptr"
                                     : std::to_string(collection->type)));
            } else if (validCollection(collection->valuestring)) {
                if (std::strncmp(collection->valuestring,
                                 DefaultCollectionIdentifier.data(),
                                 DefaultCollectionIdentifier.size()) == 0) {
                    defaultCollectionExists = true;
                }
                collections.push_back(collection->valuestring);
            } else {
                throw std::invalid_argument(
                        "Manifest::Manifest invalid collection name:" +
                        std::string(collection->valuestring));
            }
        }
    }
}

bool Manifest::validSeparator(const char* separator) {
    size_t size = std::strlen(separator);
    return size > 0 && size <= 250;
}

bool Manifest::validCollection(const char* collection) {
    // Current validation is to just check the prefix to ensure
    // 1. $default is the only $ prefixed collection.
    // 2. _ is not allowed as the first character.

    if (collection[0] == '$' && !(DefaultCollectionIdentifier == collection)) {
        return false;
    }
    return collection[0] != '_';
}

void Manifest::dump() const {
    std::cerr << *this << std::endl;
}

std::ostream& operator<<(std::ostream& os, const Manifest& manifest) {
    os << "Collections::Manifest"
       << ": revision:" << manifest.revision
       << ", defaultCollectionExists:" << manifest.defaultCollectionExists
       << ", separator:" << manifest.separator
       << ", collections.size:" << manifest.collections.size() << std::endl;
    for (const auto& entry : manifest.collections) {
        os << "collection:" << entry << "\n";
    }
    return os;
}
}
