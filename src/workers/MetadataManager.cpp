//
// Created by alexfxzhang on 2021/8/14.
//

#include "MetadataManager.h"
#include <unistd.h>
#include <sys/stat.h>
#include <sys/fcntl.h>

#define METADIR "/meta"

/**
 * 从对应目录下面init获取元数据
 * @param path
 * @return
 */
bool MetadataManager::init(const std::string &path) {
  std::string metaPath = path + METADIR;
  int ret = mkdir(metaPath.c_str(), ACCESSPERMS);
  if (ret && errno == EEXIST) {
    LogDebug("dir: %s exists", metaPath.c_str());
  } else if (ret) {
    LogError("create dir: %s error ret: %d, info: %s: ", metaPath.c_str(), ret, strerror(errno));
    return false;
  } else {
    LogInfo("create dir success: %s", metaPath.c_str());
  }
  // 初始化元数据信息
  // maxSuccessChunkId
  string successChunkIdFilePath = metaPath + SLASH_SEPARATOR + SuccessChunkIdName;
  successChunkIdFileFd = open(successChunkIdFilePath.c_str(), O_CREAT | O_RDWR);
  char buf[20];
  read(successChunkIdFileFd, buf, 20);
  successChunkIndex = atoi(buf);
  //



  // currentBitMap信息



  return true;
}

int MetadataManager::run() {
  while (m_threadstate) {
    usleep(500 * 1000);
    // 保存元数据前要加锁禁止其他线程更新对应元数据
    std::lock_guard<std::mutex> lock(_mutex);
    // 保存完成的 loaddata file 信息 （独立逻辑）
    // 认为是一个 index 信息，大于该index的需要继续load，小于的已经被load过

    // 保存 bitmap 和对应的 chunkid

    // 先bitmap，再 chunkid
    // 写最新的 minChunkId
    char buf[20] = {0};
    sprintf(buf, "%d", successChunkIndex);
    pwrite(successChunkIndex, buf, 20, 0);

  }
  return 0;
}