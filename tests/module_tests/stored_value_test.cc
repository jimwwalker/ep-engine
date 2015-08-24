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

#include "stored-value.h"

EPStats stats;

time_t time_offset;

extern "C" {
    static rel_time_t basic_current_time(void) {
        return 0;
    }

    rel_time_t (*ep_current_time)() = basic_current_time;

    time_t ep_real_time() {
        return time(NULL) + time_offset;
    }
}

static void createAndCheck(const Item& item,
                           StoredValueFactory &factory,
                           HashTable& hashTable) {
     StoredValue* value = factory(item, nullptr, hashTable);
     cb_assert(value->getBucketId() == 0);
     cb_assert(value->getKeyLen() == item.getKeyLen());
     cb_assert(value->getHashKeyLen() ==  item.getItemKey().getHashKeyLen());
     cb_assert(StoredValue::getRequiredStorage(item) == value->getObjectSize());
     cb_assert(memcmp(value->getHashKey(),
               item.getHashKey(),
               value->getHashKeyLen())==0);

     delete value;
}

int main() {
    char* env = strdup("ALLOW_NO_STATS_UPDATE=yeah");
    putenv(env);
    const int keys = 255;
    StoredValueFactory factory(stats);
    HashTable hashTable(stats);
    const int dataSize = 100;
    char data[dataSize];
    for (int ii = 0; ii < keys; ii++) {
        std::string k(ii+1, 'a');
        ItemKey itemKey(k.c_str(), ii+1, ii);
        Item item(itemKey, 0, 0, data, dataSize);
        createAndCheck(item, factory, hashTable);
    }
    free(env);
    return 0;
}