//
// Created by alexfxzhang on 2021/8/12.
//

#include "LoadFileWriter.h"
#include "utils/logger.h"


int LoadFileWriter::run() {
  while (m_threadstate) {
    LineRecord *line = nullptr;
    lineQueue->dequeue(1, line);
    if (line == nullptr && size > 0) {
      switchLoadFile();
      continue;
    } else if (line == nullptr) {
      LogInfo("get empty line, tableName: %s", tableName.c_str());
      continue;
    }
    if (size + line->size > maxFileSize) {
      switchLoadFile();
    }
    memcpy(curFilePtr + size, line->memFile, line->size);
    if (line->datetimeStartPos > 0 && *(curFilePtr + size + line->datetimeStartPos) == 'h') {
      *(curFilePtr + size + line->datetimeStartPos) = '2';
    }
    size += line->size;
  }
  return 0;
}

void LoadFileWriter::switchLoadFile() {
  if (size == 0) {
    return;
  }
  munmap(fileStartPtr, size);
  // 文件进队，待 load
  dstFileQueue->enqueue(curFileName);
  fileIndex++;
  curFileName = to_string(tableId) + "_" + to_string(fileIndex);
  int fd = open(curFileName.c_str(), O_WRONLY);
  fileStartPtr = static_cast<char *>(mmap(nullptr, maxFileSize, PROT_WRITE, MAP_SHARED, fd, 0));
  close(fd);
  size = 0;
}