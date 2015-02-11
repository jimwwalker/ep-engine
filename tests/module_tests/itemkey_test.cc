/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "config.h"
#include "itemkey.h"
#include <string.h>

int main() {

    ItemKey k1("a", 1, 0);
    cb_assert(k1.getKeyLen() == 1);
    ItemKey k2("32_byte_key____________________A", 32, 0);
    cb_assert(k2.getKeyLen() == 32);

    // Debug code regularly prints the client key even though there's no 0 termination in the spec.
    // ItemKey code should add 0 past the end of the key for safe printing.
    ItemKey k3("keyprintablecharacters#####", 22, 99);
    cb_assert(k3.getKeyLen() == 22);
    cb_assert(k3.getKey()[22] == 0);
    cb_assert(k3.getBucketId() == 99);
    cb_assert(k3.getHashKeyLen() >= k3.getKeyLen()); // hashkey will be bigger or the same.

    ItemKey k4 = k2;
    cb_assert(k4.getKeyLen() == 32);
    cb_assert(k4.getHashKeyLen() == k2.getHashKeyLen());
    cb_assert(memcmp(k4.getKey(), k2.getKey(), 32) == 0);
    cb_assert(memcmp(k4.getHashKey(), k2.getHashKey(), k4.getHashKeyLen()) == 0);


    return 0;
}