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

#include <memory>

#include "item.h"
#include <memcached/protocol_binary.h>

class MetaData {

protected:
    /*
     * Declare the metadata formats in protected visibility for the
     * derived classes.
     *
     * Each version extends the previous version, thus when allocated
     * you will have [V0|V1|V2].
     */
    class MetaDataV0 {
    public:
        MetaDataV0() {}

        void initialise() {
            cas = 0;
            exptime = 0;
            flags = 0;
        }

        uint64_t getCas() const {
           return cas;
        }

        void setCas(uint64_t cas) {
            this->cas = cas;
        }

        uint32_t getExptime() const {
            return exptime;
        }

        void setExptime(uint32_t exptime) {
            this->exptime = exptime;
        }
        uint32_t getFlags() const {
            return flags;
        }

        void setFlags(uint32_t flags) {
            this->flags = flags;
        }

     private:
        /*
         * V0 knows about CAS, expiry time and flags.
         */
        uint64_t cas;
        uint32_t exptime;
        uint32_t flags;
    };

    class MetaDataV1 {
    public:
        MetaDataV1() {}

        void initialise() {
            flexCode = 0;
            dataType = PROTOCOL_BINARY_RAW_BYTES; // equivalent to 0
        }

        void setFlexCode() {
            setFlexCode(FLEX_META_CODE);
        }

        void setFlexCode(uint8_t code) {
            flexCode = code;
        }

        uint8_t getFlexCode() const {
            return flexCode;
        }

        void setDataType(protocol_binary_datatypes dataType) {
            this->dataType = static_cast<uint8_t>(dataType);
        }

        protocol_binary_datatypes getDataType() const {
            return static_cast<protocol_binary_datatypes>(dataType);
        }

    private:
        /*
         * V1 is a 2 byte extension storing datatype
         *   0 - flexCode
         *   1 - dataType
         */
        uint8_t flexCode;
        uint8_t dataType;
    };

    class MetaDataV2 {
    public:
        MetaDataV2() {}

        void initialise() {
            confResMode = revision_seqno; // equivalent to zero
        }

        void setConfResMode(conflict_resolution_mode mode) {
            this->confResMode = static_cast<uint8_t>(mode);
        }

        conflict_resolution_mode getConfResMode() const {
            return static_cast<conflict_resolution_mode>(confResMode);
        }

    private:
        /*
         * V2 is a 1 byte extension storing the conflict resolution mode.
         */
        uint8_t confResMode;
    };

public:

    typedef enum {
        V0, // Cas/Exptime/Flags
        V1, // Flex code and datatype
        V2  // Conflict Resolution Mode
    } Version;

    MetaData ()
        :   metaDataV0(nullptr),
            metaDataV1(nullptr),
            metaDataV2(nullptr) {}

    /*
     * Copy constructor only copies values from other.
     */
    MetaData(const MetaData& other) {
        // Copy values not the pointers
        *metaDataV0 = *other.metaDataV0;
        *metaDataV1 = *other.metaDataV1;
        *metaDataV2 = *other.metaDataV2;
    }

    /*
     * Assignment operator only copies values from other.
     */
    MetaData& operator= (const MetaData& other) {
         // Copy values not the pointers
        *metaDataV0 = *other.metaDataV0;
        *metaDataV1 = *other.metaDataV1;
        *metaDataV2 = *other.metaDataV2;
        return *this;
    }

    virtual ~MetaData() {}

    virtual Version getVersion() const = 0;

    void setCas(uint64_t cas) {
        metaDataV0->setCas(htonll(cas));
    }

    uint64_t getCas() const {
       return ntohll(metaDataV0->getCas());
    }

    void setExptime(uint32_t exptime) {
        metaDataV0->setExptime(htonl(exptime));
    }

    uint32_t getExptime() const {
        return ntohl(metaDataV0->getExptime());
    }

    void setFlags(uint32_t flags) {
        metaDataV0->setFlags(flags); // flags are not byteswapped
    }

    uint32_t getFlags() const {
        return metaDataV0->getFlags(); // flags are not byteswapped
    }

    void setFlexCode() {
        metaDataV1->setFlexCode();
    }

    void setFlexCode(uint8_t code) {
        metaDataV1->setFlexCode(code);
    }

    uint8_t getFlexCode() const {
        return metaDataV1->getFlexCode();
    }

    /*
     * Note that setting the data type will also set the flex code.
     */
    void setDataType(protocol_binary_datatypes dataType) {
        setFlexCode();
        metaDataV1->setDataType(dataType);
    }

    protocol_binary_datatypes getDataType() const {
        return metaDataV1->getDataType();
    }

    conflict_resolution_mode getConfResMode() const {
        return metaDataV2->getConfResMode();
    }

    void setConfResMode(conflict_resolution_mode mode) {
        metaDataV2->setConfResMode(mode);
    }

    static size_t getMetaDataSize(Version version) {
        switch (version) {
            case V0:
                return sizeof(MetaDataV0);
            case V1:
                return sizeof(MetaDataV0) +
                        sizeof(MetaDataV1);
            case V2:
                return sizeof(MetaDataV0) +
                        sizeof(MetaDataV1) +
                        sizeof(MetaDataV2);
            default:
                throw std::logic_error("MetaData::getMetaDataSize unknown "
                                       "version " + std::to_string(version));
        }
    }

    /*
     * Move the metadata to sized_buf out.
     * After moving the metadata, this object is no longer
     * responsible for deleting the metadata's resources.
     */
    virtual void moveToSizedBuf(sized_buf& out) = 0;

protected:

    MetaDataV0* metaDataV0;
    MetaDataV1* metaDataV1;
    MetaDataV2* metaDataV2;
};

/*
 * Overlaid ExtendedMetaData will manage existing stored data
 * (via in-place new over a sized_buf)
 */
class OverlaidMetaData : public MetaData {
public:
    /*
     * Metadata is already allocated in a sized_buf
     */
    OverlaidMetaData(sized_buf metadata)
          : originalData(metadata),
            allocatedMetaDataV1(nullptr),
            allocatedMetaDataV2(nullptr) {
        if (metadata.size < getMetaDataSize(V0) ||
            metadata.size > getMetaDataSize(V2)) {
            throw std::logic_error("OverlaidMetaData: cannot "
                                   "construct onto buffer of "
                                   + std::to_string(metadata.size)
                                   + " bytes");
        }

        metaDataV0 = new (metadata.buf) MetaDataV0();

        // If the size extends enough to include V1 meta, overlay that.
        if (metadata.size >=
            (sizeof(MetaDataV0) + sizeof(MetaDataV1))) {
            metaDataV1 = new (metadata.buf + sizeof(MetaDataV0)) MetaDataV1();
        } else {
            // Else allocate the V1 extension.
            allocatedMetaDataV1.reset(new MetaDataV1());
            allocatedMetaDataV1->initialise();

            // Lend the pointer to the base class
            metaDataV1 = allocatedMetaDataV1.get();
        }

        // If the size extends enough to include V2 meta, overlay that.
        if (metadata.size ==
            (sizeof(MetaDataV0) + sizeof(MetaDataV1) + sizeof(MetaDataV2))) {
            metaDataV2 = new (metadata.buf + sizeof(MetaDataV0) +
                              sizeof(MetaDataV1)) MetaDataV2();
        } else {
            // Else allocate the V2 extension.
            allocatedMetaDataV2.reset(new MetaDataV2());
            allocatedMetaDataV2->initialise();

            // Lend the pointer to the base class
            metaDataV2 = allocatedMetaDataV2.get();
        }
    }

    ~OverlaidMetaData() {
        // Ensure the pointers we lent out get cleared
        if (allocatedMetaDataV1 != nullptr) {
            metaDataV1 = nullptr;
        }

        if (allocatedMetaDataV2 != nullptr) {
            metaDataV2 = nullptr;
        }

        // smart pointer will delete any allocations we made
    }

    OverlaidMetaData(const MetaData& other) = delete;

    /*
     * The version depends upon what we're overlaid onto.
     */
    MetaData::Version getVersion() const {
        if (originalData.size == sizeof(MetaDataV0)) {
            return V0;
        } else if (originalData.size == sizeof(MetaDataV0) + sizeof(MetaDataV1)) {
            return V1;
        } else {
            // Else we are V2
            return V2;
        }
    }

    void moveToSizedBuf(sized_buf& out) {
        // Merge any new allocations so we store out a single
        // contiguous V2 metadata
        size_t size = getMetaDataSize(V2);

        // This allocation is 'move' to out and is now the callers responsibility
        out.buf = new char[size]();

        // Copy the 3 allocations
        std::memcpy(out.buf, metaDataV0, sizeof(MetaDataV0));
        std::memcpy(out.buf + sizeof(MetaDataV0), metaDataV1, sizeof(MetaDataV1));
        std::memcpy(out.buf + sizeof(MetaDataV0)
                            + sizeof(MetaDataV1), metaDataV2, sizeof(MetaDataV2));
        out.size = size;
    }

private:
    // OverlaidMetaData uses pre-allocated memory for the storage.
    // we hold the orginal pointer/size via this sized_buf
    sized_buf originalData;
    std::unique_ptr<MetaDataV1> allocatedMetaDataV1;
    std::unique_ptr<MetaDataV2> allocatedMetaDataV2;
};

/*
 * Managed ExtendedMetaData allocates all underlying storage
 */
class ManagedMetaData : public MetaData {
public:
    ManagedMetaData() {
        initialise();
    }

    ManagedMetaData(const MetaData& other) = delete;

    ~ManagedMetaData() {}

    MetaData::Version getVersion() const {
        // Always a V2 metadata
        return V2;
    }

    void moveToSizedBuf(sized_buf& out) {
        out.buf = reinterpret_cast<char*>(storage.get());
        out.size = sizeof(MetaDataV0) +
                   sizeof(MetaDataV1) +
                   sizeof(MetaDataV2);

        // After this call we don't manage this data
        storage.release();
    }

private:

    void initialise() {
        storage.reset(new uint8_t[sizeof(MetaDataV0) +
                                  sizeof(MetaDataV1) +
                                  sizeof(MetaDataV2)]());

        // Now overlay onto our 'storage'
        metaDataV0 = new (storage.get()) MetaDataV0();
        metaDataV1 = new (&storage.get()[sizeof(MetaDataV0)]) MetaDataV1();
        metaDataV2 = new (&storage.get()[sizeof(MetaDataV0) +
                                         sizeof(MetaDataV1)]) MetaDataV2();

    }

    std::unique_ptr<uint8_t> storage;
};

/*
 * Create the appropriate MetaData container.
 */
class MetaDataFactory {
public:
    static std::unique_ptr<MetaData> createMetaData(sized_buf metadata) {
        return std::unique_ptr<MetaData>(new OverlaidMetaData(metadata));
    }

    static std::unique_ptr<MetaData> createMetaData() {
        return std::unique_ptr<MetaData>(new ManagedMetaData());
    }
};
