//
// Created by springliao on 2021/8/11.
//


#include "../common/Common.h"
#include "LineFilter.h"
#include "../utils/BitmapManager.hpp"
#include "../workers/LoadFileWriter.h"
#include "utils/logger.h"
#include <libpmem.h>


extern LoadFileWriterMgn *g_loadFileWriterMgn;
extern int g_maxChunkId;
extern DtsConf g_conf;

int LineFilter::run() {
  while (m_threadstate) {
    FileChunk *chunks[SET_DEFAULT_CAPACITY];
    int count = m_chunkQueue->getAndEraseNext(chunks);
    if (count == 0) {
      usleep(100 * 1000);
      continue;
    }
    LogError("%s line filter get %d chunks", getTimeStr(time(nullptr)).c_str(), count);
    long startTime = getCurrentLocalTimeStamp();
    // 有序的 chunkSet
    for (int i = 0; i < count; ++i) {
      FileChunk *chunk = chunks[i];
      bool containsTableId[8]{false};
      for (int j = chunk->m_lines.size() - 1; j >= 0; --j) {
        auto line = chunk->m_lines[j];
        auto tableId = line->tableId;
        // 实际上Bitmap只在这里出现过写操作，而 LineFilter 又是一个单线程的处理操作，因此这里可以直接调用BitmapManager的doSnapshot操作
        if (!g_bitmapManager->putIfAbsent(tableId, line->idxs) || line->operation == OPERATION::DELETE_OPERATION) {
          delete line; // 这个东西得及时处理掉
          continue;
        }
        // 把 line 给到 loadFileWriter
        // push to dst queue
        containsTableId[static_cast<int>(tableId) - 1] = true;
        g_loadFileWriterMgn->doWrite(line);
      }
      // 处理到最后一个就及时退出了
      int curChunkId = chunk->getChunkNo();
      delete chunk;
      for (int k = 0; k < 8; ++k) {
        // 存在这个表的行，则需要确认
        if (containsTableId[k]) {
          g_loadFileWriterMgn->workers[static_cast<TABLE_ID >(k + 1)]->insertChunkId(curChunkId);
        } else if (g_loadFileWriterMgn->workers[static_cast<TABLE_ID >(k + 1)]->chunkIdQueueIsEmpty()) {
          // set是空的的话，可以直接更新 metaManager
          g_metadataManager.fileSuccessLoadChunk[k] = curChunkId;
        }
      }
      if (curChunkId == g_maxChunkId) {
        g_conf.dispatchLineFinish = true;
        return 0;
      }
//      startTime = getCurrentLocalTimeStamp();
//      // 这里是 chunkId 和其 bitMap 的强对应关系，init 的时候取对应最接近小于 minSuccess 的 chunkId
//      std::string path =
//        g_conf.outputDir + SLASH_SEPARATOR + META_DIR + SLASH_SEPARATOR + BITMAP_PREFIX + to_string(curChunkId);
//      g_bitmapManager->doSnapshot(path);
//      LogError("bitMapManager.doSnapshot:%d cost %lld", curChunkId, getCurrentLocalTimeStamp() - startTime);
    }
    LogError("lineFilter deal %d chunk cost: %lld", count, getCurrentLocalTimeStamp() - startTime);
  }
  return 0;
}


