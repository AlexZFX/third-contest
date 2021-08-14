//
// Created by lesss on 2021/8/9.
//

#ifndef THIRD_CONTEST_METADATAMANAGER_H
#define THIRD_CONTEST_METADATAMANAGER_H

#include <map>
#include <string>
#include <mutex>

#include "common/FileChunk.h"
#include "../utils/BaseThread.h"

/**
 * 梳理一下一共需要保存的 metadata
 * 1. 已经拆分好的 chunk 信息，每个文件对应chunk的size，start，end。这个其实只需要对应的元信息即可。
 *    第一点这个可以不用当作是元数据信息，按照相同的逻辑，每次启动拆分出来的块都应该是一样的，只需要第一次拆分完写好，没拆分完就再拆一下
 * 2. 已经处理好的 chunk name，chunk是并行读取和解析的，但是需要被顺序进行过滤和分配
 * 3. 已经处理过的主键信息（bitmap信息），一个chunk的主键全部处理完之后，对应的 bitmap/数据 成功落盘到待 load 文件中，才认为这个 chunk 被成功处理完
 * 4. 生成的 loadfile 文件信息以及已完成的 loadfile 文件信息
 *
 * 注意：
 * 一个 chunk 生成好了 loadfile 文件，就说明对应的 chunk / bitmap 已经处理完成，可以进行落盘操作。
 * 也就是 loadfile/load 的逻辑是和前置的处理完全独立开的
 * 不关心 spliter，只记录已经完成的最大的连续chunkid，比这个小的都不再处理
 */
class MetadataManager : public BaseThread {
private:
  /* data */
  mutable std::mutex _mutex;
  int successChunkId; // 已完成的 chunkid 号，在这之前的 chunk 都不再做任何处理


public:

  int loadFileIndex;

  int successChunkIndex;

  MetadataManager(/* args */) : loadFileIndex(0), successChunkIndex(0) {};

  ~MetadataManager() {};

  bool init(const std::string &path);

  int run();

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
};

#endif // THIRD_CONTEST_METADATAMANAGER_H