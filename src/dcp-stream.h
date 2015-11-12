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

#ifndef SRC_DCP_STREAM_H_
#define SRC_DCP_STREAM_H_ 1

#include "config.h"
#include "dcp-response.h"

#include <queue>

class EventuallyPersistentEngine;
class MutationResponse;
class SetVBucketState;
class SnapshotMarker;
class DcpConsumer;
class DcpProducer;
class DcpResponse;

typedef enum {
    STREAM_UNINITIALISED,
    STREAM_PENDING,
    STREAM_BACKFILLING,
    STREAM_IN_MEMORY,
    STREAM_TAKEOVER_SEND,
    STREAM_TAKEOVER_WAIT,
    STREAM_READING,
    STREAM_DEAD
} stream_state_t;

typedef enum {
    //! The stream ended due to all items being streamed
    END_STREAM_OK,
    //! The stream closed early due to a close stream message
    END_STREAM_CLOSED,
    //! The stream closed early because the vbucket state changed
    END_STREAM_STATE,
    //! The stream closed early because the connection was disconnected
    END_STREAM_DISCONNECTED
} end_stream_status_t;

typedef enum {
    STREAM_ACTIVE,
    STREAM_NOTIFIER,
    STREAM_PASSIVE
} stream_type_t;

typedef enum {
    none,
    disk,
    memory
} snapshot_type_t;

typedef enum {
    all_processed,
    more_to_process,
    cannot_process
} process_items_error_t;

class Stream : public RCValue {
public:
    Stream(const std::string &name, uint32_t flags, uint32_t opaque,
           uint16_t vb, uint64_t start_seqno, uint64_t end_seqno,
           uint64_t vb_uuid, uint64_t snap_start_seqno,
           uint64_t snap_end_seqno);

    virtual ~Stream() {
        clear();
    }

    uint32_t getFlags() { return flags_; }

    uint16_t getVBucket() { return vb_; }

    uint32_t getOpaque() { return opaque_; }

    uint64_t getStartSeqno() { return start_seqno_; }

    uint64_t getEndSeqno() { return end_seqno_; }

    uint64_t getVBucketUUID() { return vb_uuid_; }

    uint64_t getSnapStartSeqno() { return snap_start_seqno_; }

    uint64_t getSnapEndSeqno() { return snap_end_seqno_; }

    RWLock& getStateLock() { return stateLock; }

    stream_state_t getState() const { return state_; }

    // stateLock must be held as writer
    void setState(stream_state_t state) { state_ = state; }

    stream_type_t getType() { return type_; }

    virtual void addStats(ADD_STAT add_stat, const void *c);

    virtual DcpResponse* next() = 0;

    virtual uint32_t setDead(end_stream_status_t status) = 0;

    virtual void notifySeqnoAvailable(uint64_t seqno) {}

    bool isActive() {
        return state_ != STREAM_DEAD;
    }

    void clear() {
        readyQ.clear();
    }

protected:

    class DcpResponseQueue {
    public:
        DcpResponseQueue()
          :  queueMemory(0) {}

        void push_back(DcpResponse* response) {
            WriterLockHolder lh(queueLock);
            queue.push(response);
            queueMemory += response->getMessageSize();
        }

        DcpResponse* pop_front() {
            WriterLockHolder lh(queueLock);
            DcpResponse* rval = NULL;
            if (!queue.empty()) {
                rval = queue.front();
                queue.pop();
                queueMemory -= rval->getMessageSize();
            }
            return rval;
        }

        DcpResponse* front() {
            ReaderLockHolder lh(queueLock);
            if (queue.empty()) {
                return NULL;
            } else {
                return queue.front();
            }
        }

        size_t clear() {
            WriterLockHolder lh(queueLock);
            while (!queue.empty()) {
                delete queue.front();
                queue.pop();
            }
            size_t rval = queueMemory;
            queueMemory = 0;
            return rval;
        }

        size_t getQueueMemory() {
            return queueMemory;
        }

        size_t getQueueSize() {
            ReaderLockHolder lh(queueLock);
            return queue.size();
        }

    private:
        std::queue<DcpResponse*> queue;
        RWLock queueLock;
        size_t queueMemory;
    };

    const char* stateName(stream_state_t st) const;

    const std::string &name_;
    uint32_t flags_;
    uint32_t opaque_;
    uint16_t vb_;
    uint64_t start_seqno_;
    uint64_t end_seqno_;
    uint64_t vb_uuid_;
    uint64_t snap_start_seqno_;
    uint64_t snap_end_seqno_;
    RWLock stateLock;
    stream_state_t state_;
    stream_type_t type_;

    AtomicValue<bool> itemsReady;

    DcpResponseQueue readyQ;

    const static uint64_t dcpMaxSeqno;
};

class ActiveStream : public Stream {
public:
    ActiveStream(EventuallyPersistentEngine* e, DcpProducer* p,
                 const std::string &name, uint32_t flags, uint32_t opaque,
                 uint16_t vb, uint64_t st_seqno, uint64_t en_seqno,
                 uint64_t vb_uuid, uint64_t snap_start_seqno,
                 uint64_t snap_end_seqno);

    ~ActiveStream() {
        ReaderLockHolder lh(getStateLock());
        transitionState(STREAM_DEAD);
    }

    DcpResponse* next();

    void setActive() {
        WriterLockHolder wlh(getStateLock());
        if (getState() == STREAM_PENDING) {
            transitionState(STREAM_BACKFILLING);
        }
    }

    uint32_t setDead(end_stream_status_t status);

    void notifySeqnoAvailable(uint64_t seqno);

    void snapshotMarkerAckReceived();

    void setVBucketStateAckRecieved();

    void incrBackfillRemaining(size_t by) {
        backfillRemaining += by;
    }

    void markDiskSnapshot(uint64_t startSeqno, uint64_t endSeqno);

    void backfillReceived(Item* itm);

    void completeBackfill();

    void addStats(ADD_STAT add_stat, const void *c);

    void addTakeoverStats(ADD_STAT add_stat, const void *c);

    size_t getItemsRemaining();

    const char* logHeader();

private:

    void transitionState(stream_state_t newState);

    DcpResponse* backfillPhase(stream_state_t& newState);

    DcpResponse* inMemoryPhase(stream_state_t& newState);

    DcpResponse* takeoverSendPhase(stream_state_t& newState);

    DcpResponse* takeoverWaitPhase();

    DcpResponse* deadPhase();

    DcpResponse* nextQueuedItem();

    void nextCheckpointItem();

    void snapshot(std::deque<MutationResponse*>& snapshot, bool mark);

    stream_state_t endStream(end_stream_status_t reason);

    stream_state_t scheduleBackfill();

    const char* getEndStreamStatusStr(end_stream_status_t status);

    //! The last sequence number queued from disk or memory
    uint64_t lastReadSeqno;
    //! The last sequence number sent to the network layer
    uint64_t lastSentSeqno;
    //! The last known seqno pointed to by the checkpoint cursor
    uint64_t curChkSeqno;
    //! The current vbucket state to send in the takeover stream
    vbucket_state_t takeoverState;
    //! The amount of items remaining to be read from disk
    size_t backfillRemaining;

    //! The amount of items that have been read from disk
    size_t itemsFromBackfill;
    //! The amount of items that have been read from memory
    size_t itemsFromMemory;
    //! Whether ot not this is the first snapshot marker sent
    bool firstMarkerSent;

    AtomicValue<int> waitForSnapshot;

    EventuallyPersistentEngine* engine;
    DcpProducer* producer;
    bool isBackfillTaskRunning;
};

class NotifierStream : public Stream {
public:
    NotifierStream(EventuallyPersistentEngine* e, DcpProducer* producer,
                   const std::string &name, uint32_t flags, uint32_t opaque,
                   uint16_t vb, uint64_t start_seqno, uint64_t end_seqno,
                   uint64_t vb_uuid, uint64_t snap_start_seqno,
                   uint64_t snap_end_seqno);

    ~NotifierStream() {
        ReaderLockHolder lh(getStateLock());
        transitionState(STREAM_DEAD);
    }

    DcpResponse* next();

    uint32_t setDead(end_stream_status_t status);

    void notifySeqnoAvailable(uint64_t seqno);

private:

    void transitionState(stream_state_t newState);

    DcpProducer* producer;
};

class PassiveStream : public Stream {
public:
    PassiveStream(EventuallyPersistentEngine* e, DcpConsumer* consumer,
                  const std::string &name, uint32_t flags, uint32_t opaque,
                  uint16_t vb, uint64_t start_seqno, uint64_t end_seqno,
                  uint64_t vb_uuid, uint64_t snap_start_seqno,
                  uint64_t snap_end_seqno, uint64_t vb_high_seqno);

    ~PassiveStream();

    process_items_error_t processBufferedMessages(uint32_t &processed_bytes);

    DcpResponse* next();

    uint32_t setDead(end_stream_status_t status);

    void acceptStream(uint16_t status, uint32_t add_opaque);

    void reconnectStream(RCPtr<VBucket> &vb, uint32_t new_opaque,
                         uint64_t start_seqno);

    ENGINE_ERROR_CODE messageReceived(DcpResponse* response);

    void addStats(ADD_STAT add_stat, const void *c);

    static const size_t batchSize;

private:

    ENGINE_ERROR_CODE processMutation(MutationResponse* mutation);

    ENGINE_ERROR_CODE commitMutation(MutationResponse* mutation,
                                     bool backfillPhase);

    ENGINE_ERROR_CODE processDeletion(MutationResponse* deletion);

    ENGINE_ERROR_CODE commitDeletion(MutationResponse* deletion,
                                     bool backfillPhase);

    void handleSnapshotEnd(RCPtr<VBucket>& vb, uint64_t byseqno);

    void processMarker(SnapshotMarker* marker);

    void processSetVBucketState(SetVBucketState* state);

    void transitionState(stream_state_t newState);

    const char* getEndStreamStatusStr(end_stream_status_t status);

    EventuallyPersistentEngine* engine;
    DcpConsumer* consumer;
    uint64_t last_seqno;

    uint64_t cur_snapshot_start;
    uint64_t cur_snapshot_end;
    snapshot_type_t cur_snapshot_type;
    bool cur_snapshot_ack;
    bool saveSnapshot;
    DcpResponseQueue buffer;
};

typedef SingleThreadedRCPtr<Stream> stream_t;
typedef RCPtr<PassiveStream> passive_stream_t;

#endif  // SRC_DCP_STREAM_H_
