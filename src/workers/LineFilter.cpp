//
// Created by springliao on 2021/8/11.
//


#include "../common/Common.h"
#include "LineFilter.h"
#include "../utils/BitmapManager.hpp"

int LineFilter::run() {
  while (m_threadstate) {
    FileChunk *chunks[10];
    int count = m_chunkQueue->getAndEraseNext(chunks);
    if (count == 0) {
      usleep(100 * 1000);
      continue;
    }
    // 有序的 chunkSet
    for (int i = 0; i < count; ++i) {
      FileChunk *chunk = chunks[i];
      if (chunk == nullptr) {
        continue;
      }
      while (!chunk->m_lines.empty()) {
        auto line = chunk->m_lines.top();
        auto tableId = line->table;
        // 存在的话无需处理了
        if (!g_bitmapManager->putIfAbsent(tableId, line->idxs)) {
          delete line; // 这个东西得及时处理掉
          continue;
        }
        // 把 line 给到 loadFileWriter
        // push to dst queue
        chunk->m_lines.pop();
      }
      // bitmapSnapShot
      // 写到 bitMapManger -> queue -> pop
      // SnapShot->id = chunkId

      // 所有 loadFileWriter -> min(chunkId) > queuePop;

    }
  }
  return 0;
}


