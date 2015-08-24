/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#ifndef SRC_COUCH_KVSTORE_COUCH_KVSTORE_H_
#define SRC_COUCH_KVSTORE_COUCH_KVSTORE_H_ 1

#include "config.h"
#include "libcouchstore/couch_db.h"

#include <map>
#include <string>
#include <vector>

#include "configuration.h"
#include "couch-kvstore/couch-fs-stats.h"
#include "histo.h"
#include "item.h"
#include "kvstore.h"
#include "tasks.h"
#include "atomicqueue.h"

#define COUCHSTORE_NO_OPTIONS 0

/**
 * Stats and timings for couchKVStore
 */
class CouchKVStoreStats {

public:
    /**
     * Default constructor
     */
    CouchKVStoreStats() :
      docsCommitted(0), numOpen(0), numClose(0),
      numLoadedVb(0), numGetFailure(0), numSetFailure(0),
      numDelFailure(0), numOpenFailure(0), numVbSetFailure(0),
      io_num_read(0), io_num_write(0), io_read_bytes(0), io_write_bytes(0),
      readSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
      writeSizeHisto(ExponentialGenerator<size_t>(1, 2), 25) {
    }

    void reset() {
        docsCommitted.store(0);
        numOpen.store(0);
        numClose.store(0);
        numLoadedVb.store(0);
        numGetFailure.store(0);
        numSetFailure.store(0);
        numDelFailure.store(0);
        numOpenFailure.store(0);
        numVbSetFailure.store(0);

        readTimeHisto.reset();
        readSizeHisto.reset();
        writeTimeHisto.reset();
        writeSizeHisto.reset();
        delTimeHisto.reset();
        compactHisto.reset();
        snapshotHisto.reset();
        commitHisto.reset();
        saveDocsHisto.reset();
        batchSize.reset();
        fsStats.reset();
    }

    // the number of docs committed
    AtomicValue<size_t> docsCommitted;
    // the number of open() calls
    AtomicValue<size_t> numOpen;
    // the number of close() calls
    AtomicValue<size_t> numClose;
    // the number of vbuckets loaded
    AtomicValue<size_t> numLoadedVb;

    //stats tracking failures
    AtomicValue<size_t> numGetFailure;
    AtomicValue<size_t> numSetFailure;
    AtomicValue<size_t> numDelFailure;
    AtomicValue<size_t> numOpenFailure;
    AtomicValue<size_t> numVbSetFailure;

    //! Number of read related io operations
    AtomicValue<size_t> io_num_read;
    //! Number of write related io operations
    AtomicValue<size_t> io_num_write;
    //! Number of bytes read
    AtomicValue<size_t> io_read_bytes;
    //! Number of bytes written
    AtomicValue<size_t> io_write_bytes;

    /* for flush and vb delete, no error handling in CouchKVStore, such
     * failure should be tracked in MC-engine  */

    // How long it takes us to complete a read
    Histogram<hrtime_t> readTimeHisto;
    // How big are our reads?
    Histogram<size_t> readSizeHisto;
    // How long it takes us to complete a write
    Histogram<hrtime_t> writeTimeHisto;
    // How big are our writes?
    Histogram<size_t> writeSizeHisto;
    // Time spent in delete() calls.
    Histogram<hrtime_t> delTimeHisto;
    // Time spent in couchstore commit
    Histogram<hrtime_t> commitHisto;
    // Time spent in couchstore compaction
    Histogram<hrtime_t> compactHisto;
    // Time spent in couchstore save documents
    Histogram<hrtime_t> saveDocsHisto;
    // Batch size of saveDocs calls
    Histogram<size_t> batchSize;
    //Time spent in vbucket snapshot
    Histogram<hrtime_t> snapshotHisto;

    // Stats from the underlying OS file operations done by couchstore.
    CouchstoreStats fsStats;
};

class EventuallyPersistentEngine;

// Additional 3 Bytes for flex meta, datatype and conflict resolution mode
const size_t COUCHSTORE_METADATA_SIZE(2 * sizeof(uint32_t) + sizeof(uint64_t) +
                                      FLEX_DATA_OFFSET + EXT_META_LEN +
                                      CONFLICT_RES_META_LEN);

/**
 * Class representing a document to be persisted in couchstore.
 */
class CouchRequest : public IORequest
{
public:
    /**
     * Constructor
     *
     * @param it Item instance to be persisted
     * @param rev vbucket database revision number
     * @param cb persistence callback
     * @param del flag indicating if it is an item deletion or not
     */
    CouchRequest(const Item &it, uint64_t rev, MutationRequestCallback &cb,
                 bool del);

    /**
     * Get the revision number of the vbucket database file
     * where the document is persisted
     *
     * @return revision number of the corresponding vbucket database file
     */
    uint64_t getRevNum(void) {
        return fileRevNum;
    }

    /**
     * Get the couchstore Doc instance of a document to be persisted
     *
     * @return pointer to the couchstore Doc instance of a document
     */
    void *getDbDoc(void) {
        if (deleteItem) {
            return NULL;
        } else {
            return &dbDoc;
        }
    }

    /**
     * Get the couchstore DocInfo instance of a document to be persisted
     *
     * @return pointer to the couchstore DocInfo instance of a document
     */
    DocInfo *getDbDocInfo(void) {
        return &dbDocInfo;
    }

    /**
     * Get the length of a document body to be persisted
     *
     * @return length of a document body
     */
    size_t getNBytes() {
        return dbDocInfo.rev_meta.size + dbDocInfo.size;
    }

private :
    value_t value;
    uint8_t meta[COUCHSTORE_METADATA_SIZE];
    uint64_t fileRevNum;
    Doc dbDoc;
    DocInfo dbDocInfo;
};

/**
 * KVStore with couchstore as the underlying storage system
 */
class CouchKVStore : public KVStore
{
public:
    /**
     * Constructor
     *
     * @param config    KVStoreConfig information.
     * @param bucketId  The id of the bucket this CouchKVStore manages.
     * @param read_only flag indicating if this kvstore instance is for
                        read-only operations
     */
    CouchKVStore(KVStoreConfig &config,
                 bucket_id_t bucketId,
                 bool read_only = false);

    /**
     * Copy constructor
     *
     * @param from the source kvstore instance
     */
    CouchKVStore(const CouchKVStore &from);

    /**
     * Deconstructor
     */
    ~CouchKVStore();

    void initialize();

    /**
     * Reset database to a clean state.
     */
    void reset(uint16_t vbucketId);

    /**
     * Begin a transaction (if not already in one).
     *
     * @return true if the transaction is started successfully
     */
    bool begin(void) {
        cb_assert(!isReadOnly());
        intransaction = true;
        return intransaction;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * @return true if the commit is completed successfully.
     */
    bool commit(Callback<kvstats_ctx> *cb);

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback(void) {
        cb_assert(!isReadOnly());
        if (intransaction) {
            intransaction = false;
        }
    }

    /**
     * Query the properties of the underlying storage.
     *
     * @return properties of the underlying storage system
     */
    StorageProperties getStorageProperties(void);

    /**
     * Insert or update a given document.
     *
     * @param itm instance representing the document to be inserted or updated
     * @param cb callback instance for SET
     */
    void set(const Item &itm, Callback<mutation_result> &cb);

    /**
     * Retrieve the document with a given key from the underlying storage system.
     *
     * @param key the key of a document to be retrieved
     * @param vb vbucket id of a document
     * @param cb callback instance for GET
     * @param fetchDelete True if we want to retrieve a deleted item if it not
     *        purged yet.
     */
    void get(const ItemKey &key, uint16_t vb, Callback<GetValue> &cb,
             bool fetchDelete = false);

    void getWithHeader(void *dbHandle, const ItemKey &key,
                       uint16_t vb, Callback<GetValue> &cb,
                       bool fetchDelete = false);

    /**
     * Retrieve the multiple documents from the underlying storage system at once.
     *
     * @param vb vbucket id of a document
     * @param itms list of items whose documents are going to be retrieved
     */
    void getMulti(uint16_t vb, vb_bgfetch_queue_t &itms);

    /**
     * Delete a given document from the underlying storage system.
     *
     * @param itm instance representing the document to be deleted
     * @param cb callback instance for DELETE
     */
    void del(const Item &itm, Callback<int> &cb);

    /**
     * Delete a given vbucket database instance from the underlying storage system
     *
     * @param vbucket vbucket id
     * @param recreate flag to re-create vbucket after deletion
     */
    void delVBucket(uint16_t vbucket);

    /**
     * Retrieve the list of persisted vbucket states
     *
     * @return vbucket state vector instance where key is vbucket id and
     * value is vbucket state
     */
   std::vector<vbucket_state *>  listPersistedVbuckets(void);

    /**
     * Retrieve ths list of persisted engine stats
     *
     * @param stats map instance where the persisted engine stats will be added
     */
    void getPersistedStats(std::map<std::string, std::string> &stats);

    /**
     * Persist a snapshot of the engine stats in the underlying storage.
     *
     * @param engine_stats map instance that contains all the engine stats
     * @return true if the snapshot is done successfully
     */
    bool snapshotStats(const std::map<std::string, std::string> &engine_stats);

    /**
     * Persist a snapshot of the vbucket states in the underlying storage system.
     *
     * @param vbucketId vbucket id
     * @param vbstate vbucket state
     * @param cb - call back for updating kv stats
     * @return true if the snapshot is done successfully
     */
    bool snapshotVBucket(uint16_t vbucketId, vbucket_state &vbstate,
                         Callback<kvstats_ctx> *cb);

     /**
     * Compact a vbucket in the underlying storage system.
     *
     * @param vbid   - which vbucket needs to be compacted
     * @param hook_ctx - details of vbucket which needs to be compacted
     * @param cb - callback to help process newly expired items
     * @param kvcb - callback to update kvstore stats
     * @return true if successful
     */
    bool compactVBucket(const uint16_t vbid, compaction_ctx *cookie,
                        Callback<kvstats_ctx> &kvcb);

    vbucket_state *getVBucketState(uint16_t vbid);

    ENGINE_ERROR_CODE updateVBState(uint16_t vbucketId,
                                    uint64_t maxDeletedRevSeqno,
                                    uint64_t snapStartSeqno,
                                    uint64_t snapEndSeqno,
                                    uint64_t maxCas,
                                    uint64_t driftCounter);

    /**
     * Does the underlying storage system support key-only retrieval operations?
     *
     * @return true if key-only retrieval is supported
     */
    bool isKeyDumpSupported() {
        return true;
    }

    /**
     * Get the number of deleted items that are persisted to a vbucket file
     *
     * @param vbid The vbucket if of the file to get the number of deletes for
     */
    size_t getNumPersistedDeletes(uint16_t vbid);

    /**
     * Get the vbucket pertaining stats from a vbucket database file
     *
     * @param vbid The vbucket of the file to get the number of docs for
     */
    DBFileInfo getDbFileInfo(uint16_t vbid);

    /**
     * Get the number of non-deleted items from a vbucket database file
     *
     * @param vbid The vbucket of the file to get the number of docs for
     * @param min_seq The sequence number to start the count from
     * @param max_seq The sequence number to stop the count at
     */
    size_t getNumItems(uint16_t vbid, uint64_t min_seq, uint64_t max_seq);

    /**
     * Do a rollback to the specified seqNo on the particular vbucket
     *
     * @param vbid The vbucket of the file that's to be rolled back
     * @param rollbackSeqno The sequence number upto which the engine needs
     * to be rolled back
     * @param cb getvalue callback
     */
    RollbackResult rollback(uint16_t vbid, uint64_t rollbackSeqno,
                            shared_ptr<RollbackCB> cb);

    /**
     * Perform pending tasks after persisting dirty items
     */
    void pendingTasks();

    /**
     * Add all the kvstore stats to the stat response
     *
     * @param prefix stat name prefix
     * @param add_stat upstream function that allows us to add a stat to the response
     * @param cookie upstream connection cookie
     */
    void addStats(const std::string &prefix, ADD_STAT add_stat, const void *cookie);

    /**
     * Add all the kvstore timings stats to the stat response
     *
     * @param prefix stat name prefix
     * @param add_stat upstream function that allows us to add a stat to the response
     * @param cookie upstream connection cookie
     */
    void addTimingStats(const std::string &prefix, ADD_STAT add_stat,
                        const void *c);

    /**
     * Resets couchstore stats
     */
    void resetStats() {
        st.reset();
    }

    static int recordDbDump(Db *db, DocInfo *docinfo, void *ctx);
    static int recordDbStat(Db *db, DocInfo *docinfo, void *ctx);
    static int getMultiCb(Db *db, DocInfo *docinfo, void *ctx);
    void readVBState(Db *db, uint16_t vbId);

    couchstore_error_t fetchDoc(Db *db, DocInfo *docinfo,
                                GetValue &docValue, uint16_t vbId,
                                bool metaOnly, bool fetchDelete = false);
    ENGINE_ERROR_CODE couchErr2EngineErr(couchstore_error_t errCode);

    CouchKVStoreStats &getCKVStoreStat(void) { return st; }

    uint64_t getLastPersistedSeqno(uint16_t vbid);

    /**
     * Get all_docs API, to return the list of all keys in the store
     */
    ENGINE_ERROR_CODE getAllKeys(uint16_t vbid, std::string &start_key,
                                 uint32_t count,
                                 shared_ptr<Callback<uint16_t&, char*&> > cb);

    ScanContext* initScanContext(shared_ptr<Callback<GetValue> > cb,
                                 shared_ptr<Callback<CacheLookup> > cl,
                                 uint16_t vbid, uint64_t startSeqno,
                                 DocumentFilter options,
                                 ValueFilter valOptions);

    scan_error_t scan(ScanContext* sctx);

    void destroyScanContext(ScanContext* ctx);

    bucket_id_t getBucketId() {
        return bucketId;
    }

private:

    bool setVBucketState(uint16_t vbucketId, vbucket_state &vbstate,
                         Callback<kvstats_ctx> *cb, bool reset=false);
    bool resetVBucket(uint16_t vbucketId, vbucket_state &vbstate) {
        cachedDocCount[vbucketId] = 0;
        return setVBucketState(vbucketId, vbstate, NULL, true);
    }

    template <typename T>
    void addStat(const std::string &prefix, const char *nm, T &val,
                 ADD_STAT add_stat, const void *c);

    void operator=(const CouchKVStore &from);

    void close();
    bool commit2couchstore(Callback<kvstats_ctx> *cb);

    uint64_t checkNewRevNum(std::string &dbname, bool newFile = false);
    void populateFileNameMap(std::vector<std::string> &filenames,
                             std::vector<uint16_t> *vbids);
    void remVBucketFromDbFileMap(uint16_t vbucketId);
    void updateDbFileMap(uint16_t vbucketId, uint64_t newFileRev);
    couchstore_error_t openDB(uint16_t vbucketId, uint64_t fileRev, Db **db,
                              uint64_t options, uint64_t *newFileRev = NULL,
                              bool reset=false);
    couchstore_error_t openDB_retry(std::string &dbfile, uint64_t options,
                                    const couch_file_ops *ops,
                                    Db **db, uint64_t *newFileRev);
    couchstore_error_t saveDocs(uint16_t vbid, uint64_t rev, Doc **docs,
                                DocInfo **docinfos, size_t docCount,
                                kvstats_ctx &kvctx);
    void commitCallback(std::vector<CouchRequest *> &committedReqs,
                        kvstats_ctx &kvctx,
                        couchstore_error_t errCode);
    couchstore_error_t saveVBState(Db *db, vbucket_state &vbState);
    void setDocsCommitted(uint16_t docs);
    void closeDatabaseHandle(Db *db);

    /**
     * Unlink selected couch file, which will be removed by the OS,
     * once all its references close.
     */
    void unlinkCouchFile(uint16_t vbucket, uint64_t fRev);

    /**
     * Remove compact file
     *
     * @param dbname
     * @param vbucket id
     * @param current db rev number
     */
    void removeCompactFile(const std::string &dbname, uint16_t vbid,
                           uint64_t currentRev);

    void removeCompactFile(const std::string &filename);

    const std::string dbname;
    std::vector<uint64_t>dbFileRevMap;
    uint16_t numDbFiles;
    std::vector<CouchRequest *> pendingReqsQ;
    bool intransaction;

    /* all stats */
    CouchKVStoreStats   st;
    couch_file_ops statCollectingFileOps;
    /* deleted docs in each file*/
    unordered_map<uint16_t, size_t> cachedDeleteCount;
    /* non-deleted docs in each file */
    unordered_map<uint16_t, size_t> cachedDocCount;
    /* pending file deletions */
    AtomicQueue<std::string> pendingFileDeletions;

    AtomicValue<size_t> backfillCounter;
    std::map<size_t, Db*> backfills;
    Mutex backfillLock;
    bucket_id_t bucketId;
};

#endif  // SRC_COUCH_KVSTORE_COUCH_KVSTORE_H_
