/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#ifndef SRC_DCP_RESPONSE_H_
#define SRC_DCP_RESPONSE_H_ 1

#include "config.h"

#include "ext_meta_parser.h"
#include "item.h"
#include "systemevent.h"

enum class DcpEvent : uint8_t {
    Mutation,
    Deletion,
    Expiration,
    Flush,
    SetVbucket,
    StreamReq,
    StreamEnd,
    SnapshotMarker,
    AddStream,
    SystemEvent
};

std::string to_string(const DcpEvent event);

enum dcp_marker_flag_t {
    MARKER_FLAG_MEMORY = 0x01,
    MARKER_FLAG_DISK   = 0x02,
    MARKER_FLAG_CHK    = 0x04,
    MARKER_FLAG_ACK    = 0x08
};

class DcpResponse {
public:
    DcpResponse(DcpEvent event, uint32_t opaque)
        : opaque_(opaque), event_(event) {}

    virtual ~DcpResponse() {}

    uint32_t getOpaque() {
        return opaque_;
    }

    DcpEvent getEvent() {
        return event_;
    }

    /* Returns true if this response is a meta event (i.e. not an operation on
     * an actual user document.
     */
    bool isMetaEvent() const {
        switch (event_) {
        case DcpEvent::Mutation:
        case DcpEvent::Deletion:
        case DcpEvent::Expiration:
        case DcpEvent::Flush:
            return false;

        case DcpEvent::SetVbucket:
        case DcpEvent::StreamReq:
        case DcpEvent::StreamEnd:
        case DcpEvent::SnapshotMarker:
        case DcpEvent::AddStream:
        case DcpEvent::SystemEvent:
            return true;
        }
        throw std::invalid_argument(
                "DcpResponse::isMetaEvent: Invalid event_ " +
                std::to_string(int(event_)));
    }

    virtual uint32_t getMessageSize() = 0;

private:
    uint32_t opaque_;
    DcpEvent event_;
};

class StreamRequest : public DcpResponse {
public:
    StreamRequest(uint16_t vbucket, uint32_t opaque, uint32_t flags,
                  uint64_t startSeqno, uint64_t endSeqno, uint64_t vbucketUUID,
                  uint64_t snapStartSeqno, uint64_t snapEndSeqno)
        : DcpResponse(DcpEvent::StreamReq, opaque), startSeqno_(startSeqno),
          endSeqno_(endSeqno), vbucketUUID_(vbucketUUID),
          snapStartSeqno_(snapStartSeqno), snapEndSeqno_(snapEndSeqno),
          flags_(flags), vbucket_(vbucket) {}

    ~StreamRequest() {}

    uint16_t getVBucket() {
        return vbucket_;
    }

    uint32_t getFlags() {
        return flags_;
    }

    uint64_t getStartSeqno() {
        return startSeqno_;
    }

    uint64_t getEndSeqno() {
        return endSeqno_;
    }

    uint64_t getVBucketUUID() {
        return vbucketUUID_;
    }

    uint64_t getSnapStartSeqno() {
        return snapStartSeqno_;
    }

    uint64_t getSnapEndSeqno() {
        return snapEndSeqno_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint64_t startSeqno_;
    uint64_t endSeqno_;
    uint64_t vbucketUUID_;
    uint64_t snapStartSeqno_;
    uint64_t snapEndSeqno_;
    uint32_t flags_;
    uint16_t vbucket_;
};

class AddStreamResponse : public DcpResponse {
public:
    AddStreamResponse(uint32_t opaque, uint32_t streamOpaque, uint16_t status)
        : DcpResponse(DcpEvent::AddStream, opaque), streamOpaque_(streamOpaque),
          status_(status) {}

    ~AddStreamResponse() {}

    uint32_t getStreamOpaque() {
        return streamOpaque_;
    }

    uint16_t getStatus() {
        return status_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint32_t streamOpaque_;
    uint16_t status_;
};

class SnapshotMarkerResponse : public DcpResponse {
public:
    SnapshotMarkerResponse(uint32_t opaque, uint16_t status)
        : DcpResponse(DcpEvent::SnapshotMarker, opaque), status_(status) {}

    uint16_t getStatus() {
        return status_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint32_t status_;
};

class SetVBucketStateResponse : public DcpResponse {
public:
    SetVBucketStateResponse(uint32_t opaque, uint16_t status)
        : DcpResponse(DcpEvent::SetVbucket, opaque), status_(status) {}

    uint16_t getStatus() {
        return status_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint32_t status_;
};

class StreamEndResponse : public DcpResponse {
public:
    StreamEndResponse(uint32_t opaque, uint32_t flags, uint16_t vbucket)
        : DcpResponse(DcpEvent::StreamEnd, opaque), flags_(flags),
          vbucket_(vbucket) {}

    uint16_t getFlags() {
        return flags_;
    }

    uint32_t getVbucket() {
        return vbucket_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint32_t flags_;
    uint16_t vbucket_;
};

class SetVBucketState : public DcpResponse {
public:
    SetVBucketState(uint32_t opaque, uint16_t vbucket, vbucket_state_t state)
        : DcpResponse(DcpEvent::SetVbucket, opaque), vbucket_(vbucket),
          state_(state) {}

    uint16_t getVBucket() {
        return vbucket_;
    }

    vbucket_state_t getState() {
        return state_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint16_t vbucket_;
    vbucket_state_t state_;
};

class SnapshotMarker : public DcpResponse {
public:
    SnapshotMarker(uint32_t opaque, uint16_t vbucket, uint64_t start_seqno,
                   uint64_t end_seqno, uint32_t flags)
        : DcpResponse(DcpEvent::SnapshotMarker, opaque), vbucket_(vbucket),
          start_seqno_(start_seqno), end_seqno_(end_seqno), flags_(flags) {}

    uint32_t getVBucket() {
        return vbucket_;
    }

    uint64_t getStartSeqno() {
        return start_seqno_;
    }

    uint64_t getEndSeqno() {
        return end_seqno_;
    }

    uint32_t getFlags() {
        return flags_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint16_t vbucket_;
    uint64_t start_seqno_;
    uint64_t end_seqno_;
    uint32_t flags_;
};

class MutationResponse : public DcpResponse {
public:
    MutationResponse(queued_item item, uint32_t opaque,
                     ExtendedMetaData *e = NULL)
        : DcpResponse(item->isDeleted() ? DcpEvent::Deletion : DcpEvent::Mutation, opaque),
          item_(item), emd(e) {}

    queued_item& getItem() {
        return item_;
    }

    Item* getItemCopy() {
        return new Item(*item_);
    }

    uint16_t getVBucket() {
        return item_->getVBucketId();
    }

    uint64_t getBySeqno() {
        return item_->getBySeqno();
    }

    uint64_t getRevSeqno() {
        return item_->getRevSeqno();
    }

    uint32_t getMessageSize() {
        const uint32_t base = item_->isDeleted() ? deletionBaseMsgBytes :
                        mutationBaseMsgBytes;
        uint32_t body = item_->getKey().size() + item_->getNBytes();

        if (emd) {
            body += emd->getExtMeta().second;
        }
        return base + body;
    }

    ExtendedMetaData* getExtMetaData() {
        return emd.get();
    }

    static const uint32_t mutationBaseMsgBytes = 55;
    static const uint32_t deletionBaseMsgBytes = 42;

private:
    queued_item item_;
    std::unique_ptr<ExtendedMetaData> emd;
};

/**
 * SystemEventMessage defines the interface required by consumer and producer
 * message classes.
 */
class SystemEventMessage : public DcpResponse {
public:
    SystemEventMessage(uint32_t opaque)
        : DcpResponse(DcpEvent::SystemEvent, opaque) {
    }
    /// @todo use sizeof(protocol_system_ev_message) once defined in memcached
    static const uint32_t baseMsgBytes =
            sizeof(protocol_binary_request_header) + sizeof(SystemEvent) +
            sizeof(uint16_t) + sizeof(int64_t);
    virtual SystemEvent getEvent() const = 0;
    virtual int64_t getBySeqno() const = 0;
    virtual uint16_t getVBucket() const = 0;
    virtual cb::const_char_buffer getKey() const = 0;
    virtual cb::const_byte_buffer getEventData() const = 0;
};

/**
 * A SystemEventConsumerMessage is used by DcpConsumer and associated code
 * for storing the data of a SystemEvent. The key and event bytes must be
 * copied from the caller into the object's storage because the consumer
 * will queue the message for future processing.
 */
class SystemEventConsumerMessage : public SystemEventMessage {
public:
    SystemEventConsumerMessage(uint32_t opaque,
                               SystemEvent ev,
                               uint64_t seqno,
                               cb::const_byte_buffer _key,
                               cb::const_byte_buffer _eventData)
        : SystemEventMessage(opaque),
          event(ev),
          bySeqno(seqno),
          key(reinterpret_cast<const char*>(_key.data()), _key.size()),
          eventData(_eventData.begin(), _eventData.end()) {
        if (seqno > std::numeric_limits<int64_t>::max()) {
            throw std::overflow_error(
                    "SystemEventMessage: overflow condition on seqno " +
                    std::to_string(seqno));
        }
    }

    uint32_t getMessageSize() override {
        return SystemEventMessage::baseMsgBytes + key.size() + eventData.size();
    }

    SystemEvent getEvent() const override {
        return event;
    }

    int64_t getBySeqno() const override {
        return bySeqno;
    }

    uint16_t getVBucket() const override {
        return vbid;
    }

    cb::const_char_buffer getKey() const override {
        return key;
    }

    cb::const_byte_buffer getEventData() const override {
        return {eventData.data(), eventData.size()};
    }

private:
    SystemEvent event;
    int64_t bySeqno;
    uint16_t vbid;
    std::string key;
    std::vector<uint8_t> eventData;
};

/**
 * CollectionsEvent provides a shim on top of SystemEventMessage for
 * when a SystemEvent is a Collection's SystemEvent.
 */
class CollectionsEvent {
public:
    CollectionsEvent(const SystemEventMessage& e) : event(e) {
    }

    const cb::const_char_buffer getCollectionName() const {
        return event.getKey();
    }

    const cb::const_char_buffer getSeparator() const {
        return event.getKey();
    }

    /**
     * @returns the revision of the collection stored in
     *          SystemEventMessage::getEventData()
     * @throws invalid_argument if the event data is not 4 bytes.
     */
    uint32_t getRevision() const {
        if (event.getEventData().size() != sizeof(uint32_t)) {
            throw std::invalid_argument(
                    "CollectionsEvent::getRevision size invalid " +
                    std::to_string(event.getEventData().size()));
        }
        return *reinterpret_cast<const uint32_t*>(event.getEventData().data());
    }

    int64_t getBySeqno() const {
        return event.getBySeqno();
    }

private:
    const SystemEventMessage& event;
};

#endif  // SRC_DCP_RESPONSE_H_
