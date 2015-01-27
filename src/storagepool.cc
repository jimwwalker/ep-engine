#include "storagepool.h"
#include "stored-value.h"


StoragePool::StoragePool() : hashTables(1024) /* TYNSET: use config values. */ {

}

StoragePool::~StoragePool() {
    hashTables.clear();
}

/**
    Return a HashTable reference for the given vbucket ID (vbid).
    The storage pool creates each HashTable on the first request, then
    returns the HashTable for all subsequent callers.
**/
HashTable& StoragePool::getOrCreateHashTable(uint16_t vbid) {
    if(hashTables[vbid].get() == nullptr) {
        hashTables[vbid] = std::unique_ptr<HashTable>(new HashTable());
    }
    return (*hashTables[vbid].get());
}

StoragePool StoragePool::thePool;

/**
    Basic factory that returns one storage pool.

    Future: support many storage pools.
**/
StoragePool& StoragePool::getStoragePool() {
    return thePool;
}
