//
// Created by alexfxzhang on 2021/8/14.
//

#include "MetadataManager.h"
#include <cstring>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "../utils/BitmapManager.hpp"
#include "common/DtsConf.h"
#include "utils/Util.h"
#include "utils/ThreadSafeQueue.h"

extern DtsConf g_conf;
extern int g_minChunkId;
extern ThreadSafeQueue<string> *g_loadDataFileNameQueue;

/**
 * 从对应目录下面init获取元数据
 * @param path
 * @return
 */
bool MetadataManager::init(const std::string &path) {
  int ret = mkdir(path.c_str(), ACCESSPERMS);
  if (ret && errno == EEXIST) {
    LogDebug("dir: %s exists", path.c_str());
  } else if (ret) {
    LogError("create dir: %s error ret: %d, info: %s: ", path.c_str(), ret, strerror(errno));
    return false;
  } else {
    LogInfo("create dir success: %s", path.c_str());
  }
  std::string metaPath = path + SLASH_SEPARATOR + META_DIR;
  ret = mkdir(metaPath.c_str(), ACCESSPERMS);
  if (ret && errno == EEXIST) {
    LogDebug("dir: %s exists", metaPath.c_str());
  } else if (ret) {
    LogError("create dir: %s error ret: %d, info: %s: ", metaPath.c_str(), ret, strerror(errno));
    return false;
  } else {
    LogInfo("create dir success: %s", metaPath.c_str());
  }
  std::string loadFilePath = path + SLASH_SEPARATOR + LOAD_FILE_DIR;
  ret = mkdir(loadFilePath.c_str(), ACCESSPERMS);
  if (ret && errno == EEXIST) {
    LogDebug("dir: %s exists", loadFilePath.c_str());
  } else if (ret) {
    LogError("create dir: %s error ret: %d, info: %s: ", loadFilePath.c_str(), ret, strerror(errno));
    return false;
  } else {
    LogInfo("create dir success: %s", loadFilePath.c_str());
  }
  // 初始化元数据信息
  // maxSuccessChunkId
  string successChunkIdFilePath = metaPath + SLASH_SEPARATOR + SuccessChunkIdName;
  successChunkIdFileFd = open(successChunkIdFilePath.c_str(), O_CREAT | O_RDWR, 0666);
  char buf[20]{0};
  read(successChunkIdFileFd, buf, 20);
  successChunkIndex = atoi(buf); // 这个 id 相当于 重启的时候的 minChunkIndex值
  g_minChunkId = successChunkIndex;
  // 把这些也更新掉，避免后续写成 0 了
  for (int &i : fileSuccessLoadChunk) {
    i = successChunkIndex;
  }
  LogInfo("successChunkIdFilePath: %s，successChunkIndex： %d", successChunkIdFilePath.c_str(), successChunkIndex);
  // 记录当前待 load 的文件信息，文件的 index 需要从这里开始增长
  string waitLoadFileIndexPath = metaPath + SLASH_SEPARATOR + WaitLoadFileIndex;
  LogInfo("waitLoadFileIndexPath: %s", waitLoadFileIndexPath.c_str());
  waitLoadFileIndexFileFd = open(waitLoadFileIndexPath.c_str(), O_CREAT | O_RDWR, 0666);
  char loadFileIndexBuf[sizeof(int) * 8]{0};
  read(waitLoadFileIndexFileFd, loadFileIndexBuf, sizeof(int) * 8);
  memcpy(loadFileIndex, loadFileIndexBuf, sizeof(int) * 8);
  // 提前过滤，不在 loadWorker 里过滤，避免导致数据缺失
  std::vector<std::string> existLoadFiles;
  string loadFileIndexStr;
  for (int j : loadFileIndex) {
    loadFileIndexStr.append(to_string(j)).append("-");
  }
  LogError("loadFileIndexStr : %s", loadFileIndexStr.c_str());
  getFileNames(g_conf.outputDir + SLASH_SEPARATOR + LOAD_FILE_DIR, existLoadFiles);
  // 判断还需要加载的就给到load队列里面去
  for (const auto &fileName : existLoadFiles) {
    int tableId = stoi(
      fileName.substr(fileName.find_last_of('/') + 1, fileName.find_last_of('_') - fileName.find_last_of('/') - 1));
    int idx = stoi(fileName.substr(fileName.find_last_of('_') + 1));
    if (idx <= loadFileIndex[tableId - 1]) {
      g_loadDataFileNameQueue->enqueue(fileName);
    }
  }
  LogError("%s finally init successChunkId: %d", getTimeStr(time(nullptr)).c_str(), successChunkIndex);
  return true;
}

int MetadataManager::run() {
  while (m_threadstate) {
    usleep(1000 * 1000);
    // 保存元数据前要加锁禁止其他线程更新对应元数据
    std::lock_guard<std::mutex> lock(_mutex);
    // 保存完成的 loaddata file 信息 （独立逻辑）
    // 认为是一个 index 信息，大于该index的需要继续load，小于的已经被load过
    char loadFileIndexBuf[sizeof(int) * 8]{0};
    memcpy(loadFileIndexBuf, loadFileIndex, sizeof(int) * 8);
    pwrite(waitLoadFileIndexFileFd, loadFileIndexBuf, sizeof(int) * 8, 0);
    // 保存 bitmap 和对应的 chunkid
    // 这个的执行转交给 lineFilter 自己去做看一看
    // 先bitmap，再 chunkid
    // 写最新的 minChunkId
    string fileSuccessLoadChunkStr;
    for (int j : fileSuccessLoadChunk) {
      fileSuccessLoadChunkStr.append(to_string(j)).append("-");
    }
    LogError("loadFileIndexStr : %s", fileSuccessLoadChunkStr.c_str());
    successChunkIndex = *min_element(fileSuccessLoadChunk, fileSuccessLoadChunk + 8);
    LogError("%s current successChunkId: %d", getTimeStr(time(nullptr)).c_str(), successChunkIndex);
    char buf[20] = {0};
    sprintf(buf, "%d", successChunkIndex);
    pwrite(successChunkIdFileFd, buf, 20, 0);

    fsync(waitLoadFileIndexFileFd);
    fsync(successChunkIdFileFd);
    fsync(successLoadFileNameFileFd);
  }
  return 0;
}

