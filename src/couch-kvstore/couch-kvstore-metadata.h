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

    enum class Version {
        V0, // Cas/Exptime/Flags
        V1, // Flex code and datatype
        V2  // Conflict Resolution Mode
    };

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
            case Version::V0:
                return sizeof(MetaDataV0);
            case Version::V1:
                return sizeof(MetaDataV0) +
                       sizeof(MetaDataV1);
            case Version::V2:
                return sizeof(MetaDataV0) +
                       sizeof(MetaDataV1) +
                       sizeof(MetaDataV2);
            default:
                throw std::logic_error("MetaData::getMetaDataSize unknown");
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
 * Managed ManagedMetaData allocates all storage
 */
class ManagedMetaData : public MetaData {
public:

    ManagedMetaData() {
        initialise();
    }

    ManagedMetaData(sized_buf& sbuf)
        : ManagedMetaData() {
        if (sbuf.size > getMetaDataSize(Version::V2) ||
            sbuf.size < getMetaDataSize(Version::V0)) {
            throw std::invalid_argument("ManagedMetaData(sized_buf& sbuf): "
                                        "sbuf.size(" + std::to_string(sbuf.size) +
                                        ") out of range min(" + std::to_string(getMetaDataSize(Version::V0)) +
                                        ") max(" + std::to_string(getMetaDataSize(Version::V2)) + ")");
        }
        std::memcpy(storage.get(), sbuf.buf, sbuf.size);
        storageSize = sbuf.size;
    }

    ManagedMetaData(const MetaData& other) = delete;

    ~ManagedMetaData() {}

    MetaData::Version getVersion() const {
        if (storageSize == sizeof(MetaDataV0)) {
            return Version::V0;
        } else if (storageSize == sizeof(MetaDataV0) + sizeof(MetaDataV1)) {
            return Version::V1;
        } else {
            // Else we are V2
            return Version::V2;
        }
    }

    void moveToSizedBuf(sized_buf& out) {
        out.buf = reinterpret_cast<char*>(storage.get());
        out.size = sizeof(MetaDataV0) +
                   sizeof(MetaDataV1) +
                   sizeof(MetaDataV2);

        // After this call we don't manage the heap allocation.
        storage.release();

        // Remove from MetaData base-class
        metaDataV0 = nullptr;
        metaDataV1 = nullptr;
        metaDataV2 = nullptr;
    }

private:

    void initialise() {
        storageSize = sizeof(MetaDataV0) +
                      sizeof(MetaDataV1) +
                      sizeof(MetaDataV2);
        storage.reset(new uint8_t[storageSize]());

        // Now overlay onto our 'storage'
        metaDataV0 = new (storage.get()) MetaDataV0();
        metaDataV1 = new (&storage.get()[sizeof(MetaDataV0)]) MetaDataV1();
        metaDataV2 = new (&storage.get()[sizeof(MetaDataV0) +
                                         sizeof(MetaDataV1)]) MetaDataV2();

    }

    std::unique_ptr<uint8_t[]> storage;
    size_t storageSize;
};

/*
 * Create the appropriate MetaData container.
 */
class MetaDataFactory {
public:
    static std::unique_ptr<MetaData> createMetaData(sized_buf metadata) {
        return std::unique_ptr<MetaData>(new ManagedMetaData(metadata));
    }

    static std::unique_ptr<MetaData> createMetaData() {
        return std::unique_ptr<MetaData>(new ManagedMetaData());
    }
};
