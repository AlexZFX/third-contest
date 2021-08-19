//
// Created by lesss on 2021/8/7.
//

#include <stdio.h>
#include <string.h>
#include <vector>
#include "common/DtsConf.h"

#include "../entity/Table.h"
#include "../entity/Column.h"
#include "FileReader.h"
#include "../utils/BitmapManager.hpp"
#include "../utils/logger.h"
#include "../utils/Util.h"

extern unordered_map<TABLE_ID, Table *, TABLE_ID_HASH> g_tableMap;
extern int g_minChunkId;
extern int g_maxChunkId;
extern DtsConf g_conf;


int FileReader::run() {
  int lastChunkId = 0;
  while (m_threadstate) {
    m_chunk = nullptr;
    m_chunkQueuePtr->dequeue(1, m_chunk);
    long startTime = getCurrentLocalTimeStamp();
    if (m_chunk == nullptr) {
      // read end return
      if (lastChunkId == g_maxChunkId) {
        g_conf.readerFinish = true;
      }
      if (g_conf.readerFinish) {
        // 读完了退出reader了
        break;
      }
      continue;
    }
    lastChunkId = m_chunk->getChunkNo();
    if (m_chunk->getChunkNo() <= g_minChunkId) {
      delete m_chunk;
      continue;
    }
    readChunk(m_chunk);
    while (!m_dstChunkSet->insert(m_chunk)) {
      usleep(200 * 1000);
    }
    LogError("%s reader deal chunk: %d cost: %lld", getTimeStr(time(nullptr)).c_str(), m_chunk->getChunkNo(),
             getCurrentLocalTimeStamp() - startTime);
    if (m_chunk->getChunkNo() == g_maxChunkId) {
      g_conf.readerFinish = true;
    }
  }
  return 0;
}

int FileReader::readChunk(FileChunk *chunk) {
  const long left = chunk->getStartPos();
  const long right = chunk->getEndPos();
  char *memFile = chunk->getMamFile();
  // 开始进行列处理
  long seek = left;
  while (seek <= right) {
    // 从 I D T 开始的每一列序号，第一列为 operation
    // 是 B 行的话直接不管
    if (memFile[seek] == 'B') {
      while (memFile[seek] != '\n') { // 注意可能越界
        ++seek;
      }
      ++seek; // \n
      // 可以开始处理这一行 A 和 insert 相同处理即可
      continue;
    }
    long lineSize = dealLine(memFile + seek, seek);
    seek += lineSize;
  }
  return 0;
}

/**
 * 从这里开始处理一行，目的是做一个前置过滤，减少处理量，数据直接拿出来，不做额外处理
 */
long FileReader::dealLine(char *start, long seek) {
  char *pos = start;
  char *tmpStart, *tmpEnd, *columnStart;
  // 把一行里面的几个 position，主键搞出来
  OPERATION op = getOpByDesc(*pos);
  pos += 2; // op 和 \t
  // 处理掉 schema 名
  while (*pos != '\t') {
    ++pos;
  }
  ++pos;
  tmpStart = pos;
  while (*pos != '\t') {
    ++pos;
  }
  tmpEnd = pos;
  string tableName(tmpStart, tmpEnd - tmpStart);
  // 根据不同的 table 有不同的处理方式
  TABLE_ID tableId = getTableIdByName(tableName);
  auto table = g_tableMap[tableId];
  ++pos; // \t
  columnStart = pos; // 列的真实开始位置
  int columnIndex = 0;
  string idx;
  long timeSeek = -1;
  seek += (pos - start);
  while (*pos != '\n') {
    tmpStart = pos;
    // 这里要加一个文件尾的判断，非\n
    while (seek <= m_chunk->getEndPos() && *pos != '\t' && *pos != '\n') {
      ++pos;
      ++seek;
    }
    if (*pos == '\t') {
      ++pos; // \t
      ++seek;
    } else if (seek > m_chunk->getEndPos()) {
      // 文件尾
      break;
    }
    tmpEnd = pos;
    if (table->columns[columnIndex].isPk) {
      idx.append(string(tmpStart, tmpEnd - tmpStart - 1)).append("-");
    } else if (table->columns[columnIndex].getDataType() == MYSQL_TYPE_DATETIME) {
      timeSeek = tmpStart - columnStart; // 记录一下对应的 time 所在的位置，每个表只有一个列，所以这就记录一次
    }
    ++columnIndex;
  }
//  ++pos; // \n
  // 读完了这一行，看看是否需要放到chunk的栈里面
  // 提前检验一次 pk 是否已经被处理过
  if (g_bitmapManager->checkExistsNoLock(tableId, idx)) { // 存在的话直接返回不需要处理了
    return pos - start + 1;
  }
  auto *line = new LineRecord();
  //设置本行 line 对应的 TableID
  line->tableId = tableId;
  //设置本行line对应的 chunkId 信息
  line->chunkId = m_chunk->getChunkNo();
  line->idxs = std::move(idx);
  //设置本行 line 的起始位置地址
  line->memFile = columnStart;
  // 设置这一行的数据有多长
  line->size = pos - columnStart + 1;
  line->operation = op;
  // 设置时间字段的长度信息
  line->datetimeStartPos = timeSeek;
  //本 chunk 的 line 解析信息记录
  m_chunk->addLine(line);
  return pos - start + 1;
}
