//
// Created by lesss on 2021/8/9.
//

#include <map>
#include <string>

#include "common/FileChunk.h"

class MetadataManager
{
private:
    /* data */
public:
    MetadataManager(/* args */);
    ~MetadataManager();

    /**
     * @brief 
     * 
     * @param chunk 
     */
    void registerChunkMetadataIfAbsent(FileChunk *chunk);

    /**
     * @brief 
     * 
     * @param chunk 
     */
    void updateChunkMetadata(FileChunk *chunk);

    /**
     * @brief Get the Chunk Metadata object
     * 
     * @param filename 
     * @param chunkNo 
     */
    FileChunk *getChunkMetadata(string filename, int chunkNo);

private:
    map<string, *Metadata> _chunkMetadataRepository;
};

MetadataManager::MetadataManager(/* args */)
{
}

MetadataManager::~MetadataManager()
{
}

MetadataManager::registerChunkMetadataIfAbsent(FileChunk *chunk)
{
}

MetadataManager::updateChunkMetadata(FileChunk *chunk)
{
}

FileChunk *MetadataManager::getChunkMetadata(string filename, int chunkNo)
{
}