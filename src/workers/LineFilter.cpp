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

        // 实际上Bitmap只在这里出现过写操作，而 LineFilter 又是一个单线程的处理操作，因此这里可以直接调用BitmapManager的doSnapshot操作
        if (!g_bitmapManager->putIfAbsent(tableId, line->idxs)) {
          delete line; // 这个东西得及时处理掉
          continue;
        }
        // 把 line 给到 loadFileWriter
        // push to dst queue
        chunk->m_lines.pop();
      }

      g_bitmapManager->doSnapshot();
    }
  }
  return 0;
}


