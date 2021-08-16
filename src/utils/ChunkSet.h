//
// Created by alexfxzhang on 2021/8/16.
//

#ifndef THIRD_CONTEST_CHUNKSET_H
#define THIRD_CONTEST_CHUNKSET_H

#include <mutex>
#include <set>
#include <common/FileChunk.h>

#define SET_DEFAULT_CAPACITY 30

struct ChunkCompare {
  bool operator()(const FileChunk *l, const FileChunk *r) {
    return l->getChunkNo() < r->getChunkNo();
  }
};

class ChunkSet {

private:

  mutable std::mutex m_mutex;
  std::set<FileChunk *, ChunkCompare> m_chunkSet;
  uint32_t m_capacity;
  // 初始化为 0，因为设定的第一条消息的 sequenceNum = 1
  // 记录上一个返回的最后一条连续消息的 sequenceNum
  int64_t m_sequenceNum;

public:
  // 初始化值需要调整为 minSuccessChunk值
  ChunkSet(int64_t startSeq, uint32_t capacity = SET_DEFAULT_CAPACITY) : m_capacity(capacity),
                                                                         m_sequenceNum(startSeq) {};

  bool insert(FileChunk *item) {
    std::lock_guard<std::mutex> guard(m_mutex);
    // 如果是大于容量的 sequenceNum，禁止插入
    if (item->getChunkNo() > m_sequenceNum + m_capacity) {
      return false;
    }
    m_chunkSet.insert(item);
    return true;
  };

  /**
   * 返回接下来n个连续的 fileChunk
   * @return count 为返回的 fileChunk 数量
   */
  int getAndEraseNext(FileChunk **jsonInfos) {
    std::lock_guard<std::mutex> guard(m_mutex);
    int count = 0, size = m_chunkSet.size();
    // 只要是连续的就都加进去，总量需要小于等于jsonInfos的容量，当前配置 jsonInfos 的容量等于 SET_DEFAULT_CAPACITY
    while (count < size && (*m_chunkSet.begin())->getChunkNo() == (m_sequenceNum + 1) && count < SET_DEFAULT_CAPACITY) {
      jsonInfos[count] = *m_chunkSet.begin();
      ++m_sequenceNum;
      ++count;
      m_chunkSet.erase(m_chunkSet.begin());
    }
    return count;
  };

};


#endif //THIRD_CONTEST_CHUNKSET_H
