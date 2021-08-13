//
// Created by alexfxzhang on 2021/8/14.
//

#include "MetadataManager.h"
#include <unistd.h>

bool MetadataManager::init(const std::string &path) {

}

int MetadataManager::run() {
  while (m_threadstate) {
    sleep(1);
    // 保存元数据前要加锁禁止其他线程更新对应元数据
    std::lock_guard<std::mutex> lock(_mutex);
    // 保存完成的 loaddata file 信息 （独立逻辑）
    // 保存 bitmap 和对应的 chunkid
  }
  return 0;
}