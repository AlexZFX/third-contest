//
// Created by alexfxzhang on 2021/8/12.
//

#include "LoadFileWriter.h"


int LoadFileWriter::run() {
  while (m_threadstate) {
    LineRecord *line = lineQueue->dequeue();
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
    memcpy(curFilePtr + size, line->memFile, size);
  }
  return 0;
}