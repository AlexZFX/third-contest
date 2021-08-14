//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_FILE_SPLIT_H
#define HIGHDTS_FILE_SPLIT_H

#include <string>
#include <mutex>
#include <utility>
#include <vector>
#include "../utils/BaseThread.h"
#include "../utils/ThreadSafeQueue.h"
#include "../common/FileChunk.h"

using namespace std;

//文件分割者
class FileSplitter : public BaseThread {

private:
  vector<string> waitDealFiles;

  ThreadSafeQueue<FileChunk *> *dstQueuePtr;


public:
  FileSplitter(std::vector<string> fileList, ThreadSafeQueue<FileChunk *> *queue) : waitDealFiles(std::move(fileList)),
                                                                                    dstQueuePtr(queue) {
  }

  /**
   * 开始执行文件分割
   * @return
   */
  int run() override;

};

#endif //HIGHDTS_FILE_SPLIT_H
