//
// Created by alexfxzhang on 2021/8/12.
//

#ifndef THIRD_CONTEST_LOADFILEWRITER_H
#define THIRD_CONTEST_LOADFILEWRITER_H

#include <utility>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "../lib/boost/lockfree/spsc_queue.hpp"
#include "../utils/BaseThread.h"
#include "../common/FileChunk.h"
#include "../common/Common.h"
#include "utils/ThreadSafeQueue.h"

using namespace std;

/**
 * 将行写到待loaddata处理的文件中
 *
 * 初步先实现成文件线性处理，后续可以优化成多线程并发
 */
class LoadFileWriter : public BaseThread {
private:
  string curFileName;
  string tableName;
  int fileIndex;
  int32_t maxFileSize;
  int32_t size;
  char *fileStartPtr;
  char *curFilePtr;

  ThreadSafeQueue<std::string> *dstFileQueue;

public:

  LoadFileWriter(string table, ThreadSafeQueue<std::string> *queuePtr) : tableName(std::move(table)), fileIndex(0),
                                                                         maxFileSize(LoadFileSize), size(0),
                                                                         dstFileQueue(queuePtr) {
    curFileName = tableName + "_" + to_string(fileIndex);
    int fd = open(curFileName.c_str(), O_WRONLY);
    fileStartPtr = static_cast<char *>(mmap(nullptr, maxFileSize, PROT_WRITE, MAP_SHARED, fd, 0));
    close(fd);
    curFilePtr = fileStartPtr;
  }

  int run() override;

  // 把一个处理好的line写入到文件里面
  bool write(LineRecord *line) {
    if (size + line->size > maxFileSize) {
      munmap(fileStartPtr, size);
      // 文件进队，待 load
      dstFileQueue->enqueue(curFileName);
      fileIndex++;
      curFileName = tableName + "_" + to_string(fileIndex);
      int fd = open(curFileName.c_str(), O_WRONLY);
      fileStartPtr = static_cast<char *>(mmap(nullptr, maxFileSize, PROT_WRITE, MAP_SHARED, fd, 0));
      close(fd);
    }
    memcpy(curFilePtr + size, line->dstChar, size);
  }

};


#endif //THIRD_CONTEST_LOADFILEWRITER_H
