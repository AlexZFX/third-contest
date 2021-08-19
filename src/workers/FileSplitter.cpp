//
// Created by lesss on 2021/8/7.
//

//
#include <pthread.h>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>

//自有文件头
#include "FileSplitter.h"
#include "../common/Common.h"
#include "../utils/Util.h"
#include "../utils/logger.h"
#include <libpmem.h>

using namespace std;

extern int g_maxChunkId;

int FileSplitter::run() {
  // 所有的 file 共用一个chunkid序列号，拆分后的每一个chunk都应该是一样的
  int64_t startTime = getCurrentLocalTimeStamp();
  int chunkNo = 1;
  for (const auto &name : this->waitDealFiles) {
    int64_t fileStartTime = getCurrentLocalTimeStamp();
    int fd = open(name.c_str(), O_RDONLY);
    if (fd == -1) {
      cout << "[ERROR] file can't open : " + name << endl;
      return -1;
    }
    struct stat st{};
    int ret = fstat(fd, &st);
    // 开始进行文件 mmap 映射
//    char *memFile = (char *) mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    size_t mappedLen;
    int isPmem;
    char *memFile = static_cast<char *>(pmem_map_file(name.c_str(), st.st_size, PMEM_FILE_CREATE, 0666, &mappedLen,
                                                      &isPmem));
    close(fd);
    if (memFile == MAP_FAILED) {
      // TODO 这里 mmap 失败，是否直接退出
      LogError("[ERROR] do mmap failed");
      return -1;
    }
    long start = st.st_size > PerChunkSize ? st.st_size - PerChunkSize : 0, end = st.st_size;
    // 从最后一块往前查找
    while (start > 0) {
      long startPos = start;
      long endPos = end;
      char *startPtr = memFile + start;
      while (*startPtr != '\n') {
        ++startPtr;
        ++startPos;
      }
      ++startPos;
      auto chunk = new FileChunk(chunkNo++, memFile, startPos, endPos - 1, st.st_size, false);
      dstQueuePtr->enqueue(chunk);
      end = startPos; // 后面set的时候会 -1
      start = end - PerChunkSize;
    }
    // 第一块
    auto chunk = new FileChunk(chunkNo++, memFile, 0, end - 1, st.st_size, true);
    dstQueuePtr->enqueue(chunk);
    int64_t fileEndTime = getCurrentLocalTimeStamp();
    LogDebug("splitter deal file: %s cost time %lld,", name.c_str(), fileEndTime - fileStartTime);
  }
  // 记录下最大的chunkNo
  g_maxChunkId = chunkNo - 1;
  int64_t endTime = getCurrentLocalTimeStamp();
  LogDebug("split all file cost time %lld, max chunk id: %d", endTime - startTime, g_maxChunkId);
  return 0;
}