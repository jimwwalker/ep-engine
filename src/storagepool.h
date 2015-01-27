
#pragma once
#include <memory>
#include <vector>
#include <stdint.h>

class HashTable;
class KVShard;

class EventuallyPersistentStoragePool {
public:
    EventuallyPersistentStoragePool();
    ~EventuallyPersistentStoragePool();

    HashTable& getOrCreateHashTable(uint16_t vbid);
    KVShard* getOrCreateKVShard(uint16_t shardId);

    static EventuallyPersistentStoragePool& getStoragePool();
private:
    std::vector< std::unique_ptr<HashTable> > hashTables;
    static EventuallyPersistentStoragePool thePool;
};
