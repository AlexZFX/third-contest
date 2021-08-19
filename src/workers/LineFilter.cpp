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
  char *buf = new char[PerChunkSize / 2]{0};
  while (m_threadstate) {
    int bufOff = 0;
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

        if (!g_bitmapManager->putIfAbsent(tableId, line->idxs)) {
          delete line;
          continue;
        }
        // 需要记录下来
        int n = snprintf(buf + bufOff, PerChunkSize / 2 - bufOff, "%d-%s\n", tableId, line->idxs.c_str());
        bufOff += n;
        if (line->operation == OPERATION::DELETE_OPERATION) {
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
        delete[]buf;
        return 0;
      }
      // 该chunk的pk信息落盘
      {
        startTime = getCurrentLocalTimeStamp();
        string path =
          g_conf.outputDir + SLASH_SEPARATOR + META_DIR + SLASH_SEPARATOR + CHUNK_PK_PREFIX + to_string(curChunkId);
        char *addr = (char *) pmem_map_file(path.c_str(), bufOff, PMEM_FILE_CREATE, 0666, nullptr, nullptr);
        memcpy(addr, buf, bufOff);
        pmem_unmap(addr, bufOff);
        memset(buf, 0, PerChunkSize / 2);
        bufOff = 0;
        LogError("%s: %s persist cost %lld", getTimeStr(time(nullptr)).c_str(), path.c_str(),
                 getCurrentLocalTimeStamp() - startTime);
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
  delete[]buf;
  return 0;
}


