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

#include "item.h"

enum class MetaEvent {
    CreateCollection,
    DeleteCollection
};

class MetaEventFactory {
public:
    /*
     * MetaEvents operate like mutations that only get persisted
     * with sequence number ordering. This method returns a valid
     * queued_item with an initialised Item that will evetually be
     * persisted as a meta-event.
     */
    static queued_item createQueuedItem(MetaEvent me, uint16_t vbid);
};