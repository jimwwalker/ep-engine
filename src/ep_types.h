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
#include <stdexcept>
#include <string>
#include <type_traits>

enum class GenerateBySeqno {
    No, Yes
};

typedef std::underlying_type<GenerateBySeqno>::type GenerateBySeqnoUType;

static inline std::string to_string(const GenerateBySeqno generateBySeqno) {
    switch (generateBySeqno) {
        case GenerateBySeqno::Yes:
            return "Yes";
        case GenerateBySeqno::No:
            return "No";
        default:
            throw std::invalid_argument("to_string(GenerateBySeqno) unknown " +
                    std::to_string(static_cast<GenerateBySeqnoUType>(generateBySeqno)));
            return "";
    }
}

enum class GenerateCas {
    No, Yes
};

typedef std::underlying_type<GenerateCas>::type GenerateByCasUType;

static inline std::string to_string(GenerateCas generateCas) {
    switch (generateCas) {
        case GenerateCas::Yes:
            return "Yes";
        case GenerateCas::No:
            return "No";
        default:
            throw std::invalid_argument("to_string(GenerateCas) unknown " +
                    std::to_string(static_cast<GenerateByCasUType>(generateCas)));
            return "";
    }
}
