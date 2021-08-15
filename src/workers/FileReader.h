//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_FILE_READER_H
#define HIGHDTS_FILE_READER_H

#include "../common/Common.h"
#include "../common/FileChunk.h"
#include "../utils/BaseThread.h"
#include "../utils/ThreadSafeQueue.h"

class FileReader : public BaseThread {
private:

  ThreadSafeQueue<FileChunk *> *m_chunkQueuePtr;

  ThreadSafeQueue<FileChunk *> *m_dstChunkQueuePtr;

  void dealLine(char *start);

  FileChunk * m_chunk;

public:

  explicit FileReader(ThreadSafeQueue<FileChunk *> *chunkQueuePtr, ThreadSafeQueue<FileChunk *> *dstChunkQueuePtr)
    : m_chunkQueuePtr(chunkQueuePtr), m_dstChunkQueuePtr(dstChunkQueuePtr) {};

  ~FileReader() = default;

  int run() override;

  /**
   * 开始读取文件块
   * @param chunk
   * @return
   */
  int readChunk(FileChunk *chunk);

  /**
   * 按行读取每条记录
   * @return {@link BatchLineRecord}
   */
  struct BatchLineRecord *readLines();

};

class FileReaderMgn : public BaseThread {
private:
  int m_threadNum;
  FileReader *workers[100]{};
public:
  FileReaderMgn(int threadNum, ThreadSafeQueue<FileChunk *> *queuePtr, ThreadSafeQueue<FileChunk *> *dstQueuePtr)
    : m_threadNum(
    threadNum) {
    for (int i = 0; i < m_threadNum; ++i) {
      workers[i] = new FileReader(queuePtr, dstQueuePtr);
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
  }

};

#endif //HIGHDTS_FILE_READER_H
