//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_FILE_READER_H
#define HIGHDTS_FILE_READER_H

#include "../common/Common.h"
#include "../common/FileChunk.h"
#include "../utils/BaseThread.h"
#include "../utils/ThreadSafeQueue.h"
#include "../utils/ChunkSet.h"

class FileReader : public BaseThread {
private:

  ThreadSafeQueue<FileChunk *> *m_chunkQueuePtr;

  ChunkSet *m_dstChunkSet;

  int dealLine(char *start, int seek);

  FileChunk *m_chunk{};

public:

  explicit FileReader(ThreadSafeQueue<FileChunk *> *chunkQueuePtr, ChunkSet *dstChunkSet)
    : m_chunkQueuePtr(chunkQueuePtr), m_dstChunkSet(dstChunkSet) {};

  ~FileReader() = default;

  int run() override;

  /**
   * 开始读取文件块
   * @param chunk
   * @return
   */
  int readChunk(FileChunk *chunk);

};

class FileReaderMgn : public BaseThread {
private:
  int m_threadNum;
  std::vector<FileReader *> workers;
public:
  FileReaderMgn(int threadNum, ThreadSafeQueue<FileChunk *> *queuePtr, ChunkSet *dstQueuePtr)
    : m_threadNum(threadNum) {
    for (int i = 0; i < m_threadNum; ++i) {
      workers.emplace_back(new FileReader(queuePtr, dstQueuePtr));
    }
  }

  ~FileReaderMgn() {
    for (int i = 0; i < m_threadNum; ++i) {
      delete workers[i];
    }
  }

  int run() override {
    for (int i = 0; i < m_threadNum; ++i) {
      workers[i]->start();
    }
    for (int i = 0; i < m_threadNum; ++i) {
      workers[i]->join();
    }
    return 0;
  }

};

#endif //HIGHDTS_FILE_READER_H
