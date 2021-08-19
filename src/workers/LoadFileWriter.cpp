//
// Created by alexfxzhang on 2021/8/12.
//

#include "LoadFileWriter.h"
#include "utils/logger.h"
#include "workers/MetadataManager.h"

extern DtsConf g_conf;
extern MetadataManager g_metadataManager;

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
    currentChunkId = line->chunkId;
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
//  pmem_unmap(fileStartPtr, size);
  truncate(curFileName.c_str(), size);
  LogError("%s make load file: %s cost: %lld, fileSize: %d", getTimeStr(time(nullptr)).c_str(), curFileName.c_str(),
           getCurrentLocalTimeStamp() - lastTime, size);
  lastTime = getCurrentLocalTimeStamp();
  // 文件进队，待 load
  dstFileQueue->enqueue(curFileName);
  fileIndex++;
  curFileName = g_conf.outputDir + SLASH_SEPARATOR + LOAD_FILE_DIR + SLASH_SEPARATOR +
                to_string(static_cast<int>(tableId)) + "_" + to_string(fileIndex);
  int fd = open(curFileName.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0666);
  lseek(fd, maxFileSize, SEEK_END);
  ::write(fd, "", 1);
  fileStartPtr = static_cast<char *>(mmap(nullptr, maxFileSize, PROT_WRITE, MAP_SHARED, fd, 0));
//  fileStartPtr = static_cast<char *>(pmem_map_file(curFileName.c_str(), maxFileSize, PMEM_FILE_CREATE, 0666,
//                                                   nullptr, nullptr));
  curFilePtr = fileStartPtr;
  close(fd);
  // PERSIST
  // 这里就保存待 load 的 index 进入到 meta，同时更新其对应的 chunkIndex
  // 这里存在问题是，只有这个chunk有对应数据的时候，才会更新到 chunkIndex，没有的话就不会更新这个index了
  g_metadataManager.fileSuccessLoadChunk[static_cast<int>(tableId) - 1] = currentChunkId - 1;
  // 这里保存的 fileIndex，是已经成功落盘了没问题的
  g_metadataManager.loadFileIndex[static_cast<int>(tableId) - 1] = fileIndex - 1;
  size = 0;
  // 小于这个 currentChunkId 都应该被 erase 掉
  lock_guard<mutex> guard(_mutex);
  while (!needCheckChunkIdQueue.empty() && needCheckChunkIdQueue.front() <= currentChunkId - 1) {
    needCheckChunkIdQueue.pop();
  }
  // 如果没有其他消息，那就把 currentChunkId 也remove掉
  if (lineQueue.empty() && !needCheckChunkIdQueue.empty() && needCheckChunkIdQueue.front() <= currentChunkId) {
    needCheckChunkIdQueue.pop();
    g_metadataManager.fileSuccessLoadChunk[static_cast<int>(tableId) - 1] = currentChunkId;
  }
}

int LoadFileWriter::getCurrentChunkId() const {
  return currentChunkId;
}

const string &LoadFileWriter::getCurFileName() const {
  return curFileName;
}

const string &LoadFileWriter::getTableName() const {
  return tableName;
}

TABLE_ID LoadFileWriter::getTableId() const {
  return tableId;
}

int LoadFileWriter::getFileIndex() const {
  return fileIndex;
}

int32_t LoadFileWriter::getMaxFileSize() const {
  return maxFileSize;
}

int32_t LoadFileWriter::getSize() const {
  return size;
}

char *LoadFileWriter::getFileStartPtr() const {
  return fileStartPtr;
}

char *LoadFileWriter::getCurFilePtr() const {
  return curFilePtr;
}

const ThreadSafeQueue<LineRecord *> &LoadFileWriter::getLineQueue() const {
  return lineQueue;
}

ThreadSafeQueue<std::string> *LoadFileWriter::getDstFileQueue() const {
  return dstFileQueue;
}