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
#include "../utils/Util.h"
#include "../common/Common.h"
#include "../utils/ThreadSafeQueue.h"
#include "common/DtsConf.h"
#include "boost/lockfree/spsc_queue.hpp"

using namespace std;

extern DtsConf g_conf;

/**
 * 将行写到待loaddata处理的文件中
 *
 * 初步先实现成文件线性处理，后续可以优化成多线程并发
 */
class LoadFileWriter : public BaseThread {
private:
  int currentChunkId;
  string curFileName;
  string tableName;
  TABLE_ID tableId;
  int fileIndex;
  int32_t maxFileSize;
  int32_t size;
  char *fileStartPtr;
  char *curFilePtr;

  ThreadSafeQueue<LineRecord *> lineQueue;
//  boost::lockfree::spsc_queue <LineRecord *> *lineQueue;
  ThreadSafeQueue<std::string> *dstFileQueue;

public:

  LoadFileWriter(string table, ThreadSafeQueue<std::string> *queuePtr)
    : tableName(std::move(table)), fileIndex(0),
      maxFileSize(LoadFileSize), size(0),
      dstFileQueue(queuePtr){
    tableId = getTableIdByName(tableName);
    curFileName = g_conf.outputDir + SLASH_SEPARATOR + LOAD_FILE_DIR + SLASH_SEPARATOR +
                  to_string(static_cast<int>(tableId)) + "_" + to_string(fileIndex);
    int fd = open(curFileName.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0666); // 先不 close
    lseek(fd, maxFileSize, SEEK_END);
    ::write(fd, "", 1);
    fileStartPtr = static_cast<char *>(mmap(nullptr, maxFileSize, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0));
    curFilePtr = fileStartPtr;
    close(fd);
  }

  ~LoadFileWriter() {
  }

  int run();

  // 把一个处理好的line写入到文件里面
  //TODO 这里存在一个问题，最后一个数据有可能无法满足 size 的条件进而导致存在残留数据无法处理
  bool write(LineRecord *line) {
    lineQueue.enqueue(line);
    return true;
  }

  void switchLoadFile();

};

class LoadFileWriterMgn : public BaseThread {
private:
  std::mutex _mutex;
  unordered_map<TABLE_ID, LoadFileWriter *, TABLE_ID_HASH> workers;
public:
  LoadFileWriterMgn(ThreadSafeQueue<std::string> *queuePtr) {
    for (const auto &item : g_tableMap) {
      workers[item.first] = new LoadFileWriter(item.second->getTableName(), queuePtr);
    }
  }

  ~LoadFileWriterMgn() {
    for (auto &worker : workers) {
      delete worker.second;
    }
  }

  int run() override {
    for (auto &worker : workers) {
      worker.second->start();
    }
    for (auto &worker : workers) {
      worker.second->join();
    }
    return 0;
  }

  void doWrite(LineRecord *record) {
    std::lock_guard<std::mutex> guard(_mutex);
    workers[record->tableId]->write(record);
  }
};

#endif //THIRD_CONTEST_LOADFILEWRITER_H
