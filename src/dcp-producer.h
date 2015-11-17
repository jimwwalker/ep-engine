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

#ifndef SRC_DCP_PRODUCER_H_
#define SRC_DCP_PRODUCER_H_ 1

#include "config.h"

#include "tapconnection.h"
#include "dcp-stream.h"

class DcpResponse;

class DcpProducer : public Producer {
public:

    DcpProducer(EventuallyPersistentEngine &e, const void *cookie,
                const std::string &n, bool notifyOnly);

    ENGINE_ERROR_CODE streamRequest(uint32_t flags, uint32_t opaque,
                                    uint16_t vbucket, uint64_t start_seqno,
                                    uint64_t end_seqno, uint64_t vbucket_uuid,
                                    uint64_t last_seqno, uint64_t next_seqno,
                                    uint64_t *rollback_seqno,
                                    dcp_add_failover_log callback);

    ENGINE_ERROR_CODE getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                     dcp_add_failover_log callback);

    ENGINE_ERROR_CODE step(struct dcp_message_producers* producers);

    ENGINE_ERROR_CODE bufferAcknowledgement(uint32_t opaque, uint16_t vbucket,
                                            uint32_t buffer_bytes);

    ENGINE_ERROR_CODE control(uint32_t opaque, const void* key, uint16_t nkey,
                              const void* value, uint32_t nvalue);

    ENGINE_ERROR_CODE handleResponse(protocol_binary_response_header *resp);

    void addStats(ADD_STAT add_stat, const void *c);

    void addTakeoverStats(ADD_STAT add_stat, const void* c, uint16_t vbid);

    void aggregateQueueStats(ConnCounter* aggregator);

    void setDisconnect(bool disconnect);

    void notifySeqnoAvailable(uint16_t vbucket, uint64_t seqno);

    void vbucketStateChanged(uint16_t vbucket, vbucket_state_t state);

    void closeAllStreams();

    const char *getType() const;

    bool isTimeForNoop();

    void setTimeForNoop();

    void clearQueues();

    void appendQueue(std::list<queued_item> *q);

    size_t getBackfillQueueSize();

    size_t getItemsSent();

    size_t getTotalBytes();

    bool windowIsFull();

    void flush();

    std::list<uint16_t> getVBList(void);

    /**
     * Close the stream for given vbucket stream
     *
     * @param vbucket the if for the vbucket to close
     * @return ENGINE_SUCCESS upon a successful close
     *         ENGINE_NOT_MY_VBUCKET the vbucket stream doesn't exist
     */
    ENGINE_ERROR_CODE closeStream(uint32_t opaque, uint16_t vbucket);

    void notifyStreamReady(uint16_t vbucket, bool schedule);

    class BufferLog {
    public:

        /*
            BufferLog has 3 states.
            Disabled - if no flow-control is in-use. This is indicated by setting
             the size to 0 (i.e. setBufferSize(0)).

            SpaceAvailable - there is space available in the buffer for an insert.
             Note that BufferLog has always allowed you to insert a n-byte op if
             n-1 bytes space is available.

            Full - inserts have taken the number of bytes available over the max.
        */
        enum State {
            Disabled,
            Full,
            SpaceAvailable
        };

        BufferLog()
            : maxBytes(0), bytesSent(0), ackedBytes(0) {}

        void setBufferSize(size_t maxBytes);

        void addStats(const DcpProducer& myProducer, ADD_STAT add_stat, const void *c);

        /*
            Return true if the buffer is disabled or there is space.
        */
        bool insert(size_t bytes);

        /*
            Returns the state of the log *before* acknowledgement of 'bytes'.
        */
        State acknowledge(size_t bytes);

        State getState();

    private:

        bool isEnabled_UNLOCKED() {
            return maxBytes != 0;
        }

        bool isFull_UNLOCKED() {
            return bytesSent >= maxBytes;
        }

        void release_UNLOCKED(size_t bytes);

        /*
            Get the BufferLog state without any locks.
        */
        State getState_UNLOCKED();

        RWLock logLock;
        size_t maxBytes;
        size_t bytesSent;
        size_t ackedBytes;
    };

    /*
        Insert response into producer's buffer log.

        If the log is disabled or has space returns true.
        Else return false.
    */
    bool bufferLogInsert(size_t bytes);

private:

    DcpResponse* getNextItem();

    size_t getItemsRemaining();
    stream_t findStreamByVbid(uint16_t vbid);

    ENGINE_ERROR_CODE maybeSendNoop(struct dcp_message_producers* producers);

    struct {
        rel_time_t sendTime;
        uint32_t opaque;
        uint32_t noopInterval;
        bool pendingRecv;
        bool enabled;
    } noopCtx;

    DcpResponse *rejectResp; // stash response for retry if E2BIG was hit

    bool notifyOnly;
    rel_time_t lastSendTime;
    BufferLog log;

    // Guards all accesses to streams map. If only reading elements in streams
    // (i.e. not adding / removing elements) then can acquire ReadLock, even
    // if a non-const method is called on stream_t.
    RWLock streamsMutex;

    std::vector<AtomicValue<bool> > vbReady;

    std::map<uint16_t, stream_t> streams;

    AtomicValue<size_t> itemsSent;
    AtomicValue<size_t> totalBytesSent;

    size_t roundRobinVbReady;
    static const uint32_t defaultNoopInerval;
};

#endif  // SRC_DCP_PRODUCER_H_
