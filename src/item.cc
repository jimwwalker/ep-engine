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

#include "config.h"

#include "cJSON.h"
#include "item.h"
#include "vbucket.h"

#include <platform/compress.h>

#include  <iomanip>

std::atomic<uint64_t> Item::casCounter(1);
const uint32_t Item::metaDataSize(2*sizeof(uint32_t) + 2*sizeof(uint64_t) + 2);


std::string to_string(queue_op op) {
    switch(op) {
        case queue_op::set: return "set";
        case queue_op::del: return "del";
        case queue_op::flush: return "flush";
        case queue_op::empty: return "empty";
        case queue_op::checkpoint_start: return "checkpoint_start";
        case queue_op::checkpoint_end: return "checkpoint_end";
        case queue_op::set_vbucket_state: return "set_vbucket_state";
        case queue_op::system_event: return "system_event";
    }
    return "<" +
            std::to_string(static_cast<std::underlying_type<queue_op>::type>(op)) +
            ">";

}

bool operator==(const Item& lhs, const Item& rhs) {
    return (lhs.metaData == rhs.metaData) &&
           (*lhs.value == *rhs.value) &&
           (lhs.key == rhs.key) &&
           (lhs.bySeqno == rhs.bySeqno) &&
           // Note: queuedTime is *not* compared. The rationale is it is
           // simply used for stats (measureing queue duration) and hence can
           // be ignored from an "equivilence" pov.
           // (lhs.queuedTime == rhs.queuedTime) &&
           (lhs.vbucketId == rhs.vbucketId) &&
           (lhs.op == rhs.op) &&
           (lhs.nru == rhs.nru);
}

std::ostream& operator<<(std::ostream& os, const Item& i) {
    os << "Item[" << &i << "] with"
       << " key:" << i.key << "\n"
       << "\tvalue:" << *i.value << "\n"
       << "\tmetadata:" << i.metaData << "\n"
       << "\tbySeqno:" << i.bySeqno
       << " queuedTime:" << i.queuedTime
       << " vbucketId:" << i.vbucketId
       << " op:" << to_string(i.op)
       << " nru:" << int(i.nru);
    return os;
}

bool operator==(const ItemMetaData& lhs, const ItemMetaData& rhs) {
    return (lhs.cas == rhs.cas) &&
           (lhs.revSeqno == rhs.revSeqno) &&
           (lhs.flags == rhs.flags) &&
           (lhs.exptime == rhs.exptime);
}

std::ostream& operator<<(std::ostream& os, const ItemMetaData& md) {
    os << "ItemMetaData[" << &md << "] with"
       << " cas:" << md.cas
       << " revSeqno:" << md.revSeqno
       << " flags:" << md.flags
       << " exptime:" << md.exptime;
    return os;
}

bool operator==(const Blob& lhs, const Blob& rhs) {
    return (lhs.size == rhs.size) &&
           (lhs.extMetaLen == rhs.extMetaLen) &&
           (lhs.age == rhs.age) &&
           (memcmp(lhs.data, rhs.data, lhs.size) == 0);
}

std::ostream& operator<<(std::ostream& os, const Blob& b) {
    os << "Blob[" << &b << "] with"
       << " size:" << b.size
       << " extMetaLen:" << int(b.extMetaLen)
       << " age:" << int(b.age)
       << " data: <" << std::hex;
    // Print at most 40 bytes of the body.
    auto bytes_to_print = std::min(uint32_t(40), b.size);
    for (size_t ii = 0; ii < bytes_to_print; ii++) {
        if (ii != 0) {
            os << ' ';
        }
        if (isprint(b.data[ii])) {
            os << b.data[ii];
        } else {
            os << std::setfill('0') << std::setw(2) << int(uint8_t(b.data[ii]));
        }
    }
    os << std::dec << '>';
    return os;
}

bool Item::compressValue(float minCompressionRatio) {
    auto datatype = getDataType();
    if (!mcbp::datatype::is_compressed(datatype)) {
        // Attempt compression only if datatype indicates
        // that the value is not compressed already.
        cb::compression::Buffer deflated;
        if (cb::compression::deflate(cb::compression::Algorithm::Snappy,
                                     getData(), getNBytes(), deflated)) {
            if (deflated.len > minCompressionRatio * getNBytes()) {
                // No point doing the compression if the desired
                // compression ratio isn't achieved.
                return true;
            }
            setData(deflated.data.get(), deflated.len,
                    (uint8_t *)(getExtMeta()), getExtMetaLen());

            datatype |= PROTOCOL_BINARY_DATATYPE_COMPRESSED;
            setDataType(datatype);
        } else {
            return false;
        }
    }
    return true;
}

bool Item::decompressValue() {
    uint8_t datatype = getDataType();
    if (mcbp::datatype::is_compressed(datatype)) {
        // Attempt decompression only if datatype indicates
        // that the value is compressed.
        cb::compression::Buffer inflated;
        if (cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                     getData(), getNBytes(), inflated)) {
            setData(inflated.data.get(), inflated.len,
                    (uint8_t *)(getExtMeta()), getExtMetaLen());
            datatype &= ~PROTOCOL_BINARY_DATATYPE_COMPRESSED;
            setDataType(datatype);
        } else {
            return false;
        }
    }
    return true;
}

item_info Item::toItemInfo(const VBucket* vb) const {
    item_info info;
    info.cas = getCas();
    info.vbucket_uuid = vb ? vb->failovers->getLatestUUID() : 0;
    info.seqno = getBySeqno();
    info.exptime = getExptime();
    info.nbytes = getNBytes();
    info.flags = getFlags();
    info.datatype = getDataType();

    if (isDeleted()) {
        info.document_state = DocumentState::Deleted;
    } else {
        info.document_state = DocumentState::Alive;
    }
    info.nkey = static_cast<uint16_t>(getKey().size());
    info.key = getKey().data();
    info.value[0].iov_base = const_cast<char*>(getData());
    info.value[0].iov_len = getNBytes();
    if (vb) {
        /*
         * Set the collectionLen - note this is currently an extra scan/search
         * on the key looking for the separator. Two points:
         *
         * 1. When separator changes are added this can go wrong. If the
         *    get_item_info engine call back is made with a key "c::k" and the
         *    separator has just changed from "::" to "-", we will report
         *    a collectionLen of 0, instead of 1.
         *    We would need to block separator changes if we know this bug could
         *    be possible, e.g. if DCP queue is not empty cannot change
         *    separator. Or read 2...
         *
         * 2. This extra search could be removed if all StoredValue/Item carry
         *    the collectionLen from construction onwards. Note we will have
         *    scanned the DocKey at SET time for collection validation and thus
         *    could save the collectionLen at that point removing the need for
         *    this extra scan and at the same time addressing point 1.
         *    This would be at the cost of 1 extra byte per Item/StoredValue.
         *
         * For now do the scan here.
         */
        info.collectionLen = vb->lockCollections()
                                     .makeCollectionsDocKey(key)
                                     .getCollectionLen();
    }
    return info;
}
