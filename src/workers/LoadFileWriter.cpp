//
// Created by alexfxzhang on 2021/8/12.
//

#include "LoadFileWriter.h"
#include "utils/logger.h"

extern DtsConf g_conf;

int LoadFileWriter::run() {
  while (m_threadstate) {
    LineRecord *line = nullptr;
    lineQueue.dequeue(1, line);
    if (line == nullptr && size > 0) {
      // 大于0则不进行等待，直接切换写文件
      switchLoadFile();
      size = 0;
      continue;
    } else if (line == nullptr) {
      if (g_conf.dispatchLineFinish && lineQueue.empty()) {
        munmap(fileStartPtr, maxFileSize);
        remove(curFileName.c_str());
        g_conf.loadFileWriteFinish = true;
        break;
      }
      continue;
    }
    if (line->tableId == TABLE_ID::TABLE_ORDERS_LINE_ID) {
      ++g_conf.orderLineCount;
    }
    if (size + line->size >= maxFileSize) {
      switchLoadFile();
    }
    memcpy(curFilePtr + size, line->memFile, line->size);
    if (line->datetimeStartPos > 0 && *(curFilePtr + size + line->datetimeStartPos) == 'h') {
      *(curFilePtr + size + line->datetimeStartPos) = '2';
    }
    size += line->size;
    // 每个文件的最后一行非 \n 的，需要补上
    if (*(curFilePtr + size - 1) != '\n') {
      *(curFilePtr + size) = '\n';
      ++size;
    }
    // 清理掉 line 的数据，后续都是文件了
    delete line;
  }
  return 0;
}

void LoadFileWriter::switchLoadFile() {
  if (size == 0) {
    return;
  }
  munmap(fileStartPtr, size);
  truncate(curFileName.c_str(), size);
  // 文件进队，待 load
  dstFileQueue->enqueue(curFileName);
  fileIndex++;
  curFileName = g_conf.outputDir + SLASH_SEPARATOR + LOAD_FILE_DIR + SLASH_SEPARATOR +
                to_string(static_cast<int>(tableId)) + "_" + to_string(fileIndex);
  int fd = open(curFileName.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0666);
  lseek(fd, maxFileSize, SEEK_END);
  ::write(fd, "", 1);
  fileStartPtr = static_cast<char *>(mmap(nullptr, maxFileSize, PROT_WRITE, MAP_SHARED, fd, 0));
  curFilePtr = fileStartPtr;
  close(fd);
  // PERSISTz
  size = 0;
}