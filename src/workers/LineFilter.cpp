//
// Created by springliao on 2021/8/11.
//


#include "../common/Common.h"
#include "LineFilter.h"
#include "../utils/BitmapManager.hpp"
#include "../workers/LoadFileWriter.h"
#include "utils/logger.h"

extern LoadFileWriterMgn *g_loadFileWriterMgn;
extern int g_maxChunkId;
extern DtsConf g_conf;

int LineFilter::run() {
  while (m_threadstate) {
    FileChunk *chunks[10];
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
      for (int j = chunk->m_lines.size() - 1; j >= 0; --j) {
        auto line = chunk->m_lines[j];
        auto tableId = line->tableId;
        g_conf.beforeFilterCounts[tableId] += 1;
        // 实际上Bitmap只在这里出现过写操作，而 LineFilter 又是一个单线程的处理操作，因此这里可以直接调用BitmapManager的doSnapshot操作
        if (!g_bitmapManager->putIfAbsent(tableId, line->idxs) || line->operation == OPERATION::DELETE_OPERATION) {
          delete line; // 这个东西得及时处理掉
          continue;
        }
        g_conf.afterCounts[tableId] += 1;
        // 把 line 给到 loadFileWriter
        // push to dst queue
        g_loadFileWriterMgn->doWrite(line);
      }
      // 处理到最后一个就及时退出了
      if (chunk->getChunkNo() == g_maxChunkId) {
        delete chunk;
        g_conf.dispatchLineFinish = true;
        return 0;
      } else {
        delete chunk;
      }
    }
    LogError("lineFilter deal %d chunk cost: %lld", count, getCurrentLocalTimeStamp() - startTime);
//      g_bitmapManager->doSnapshot();
  }
  return 0;
}


