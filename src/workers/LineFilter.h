//
// Created by springliao on 2021/8/11.
//

#ifndef THIRD_CONTEST_LINEFILTER_H
#define THIRD_CONTEST_LINEFILTER_H


#include "map"

#include "../common/FileChunk.h"
#include "../entity/Table.h"
#include "../utils/BaseThread.h"
#include "../utils/ThreadSafeQueue.h"
#include "../utils/ChunkSet.h"

class LineFilter : public BaseThread {
private:
  Table *table;
  ChunkSet *m_chunkQueue;

public:

  explicit LineFilter(ChunkSet *chunkQueue) : m_chunkQueue(chunkQueue) {
  }

  Table *getTable() const {
    return table;
  }

  int run();

};

#endif //THIRD_CONTEST_LINEFILTER_H
