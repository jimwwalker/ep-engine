#include "storagepool.h"
#include "stored-value.h"


EventuallyPersistentStoragePool::EventuallyPersistentStoragePool() : hashTables(1024) /*TYNSET: some config value. */ {

}

EventuallyPersistentStoragePool::~EventuallyPersistentStoragePool() {
    hashTables.clear();
}

/**
    Return a HashTable reference for the given vbucket ID (vbid).
    The storage pool creates each HashTable on the first request, then
    returns the HashTable for all subsequent callers.
**/
HashTable& EventuallyPersistentStoragePool::getOrCreateHashTable(uint16_t vbid) {
    if(hashTables[vbid].get() == nullptr) {
        hashTables[vbid] = std::unique_ptr<HashTable>(new HashTable());
    }
    return (*hashTables[vbid].get());
}

EventuallyPersistentStoragePool EventuallyPersistentStoragePool::thePool;

/**
    Basic factory that returns one storage pool.

    Future: support many storage pools.
**/
EventuallyPersistentStoragePool& EventuallyPersistentStoragePool::getStoragePool() {
    return thePool;
}