
#pragma once
#include <memory>
#include <vector>
#include <stdint.h>

class HashTable;
class KVShard;

class StoragePool {
public:
    StoragePool();
    ~StoragePool();

    HashTable& getOrCreateHashTable(uint16_t vbid);
    KVShard* getOrCreateKVShard(uint16_t shardId);

    static StoragePool& getStoragePool();
private:
    std::vector< std::unique_ptr<HashTable> > hashTables;
    static StoragePool thePool;
};
