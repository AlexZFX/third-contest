//
// Created by lesss on 2021/8/7.
//

#include <stdio.h>
#include <string.h>
#include <vector>

#include "entity/Table.h"
#include "entity/Column.h"
#include "FileReader.h"
#include "utils/BitmapManager.hpp"

extern unordered_map<string, Table *> g_tableMap;

int FileReader::run() {
  while (m_threadstate) {
    m_chunk = nullptr;
    m_chunkQueuePtr->dequeue(1, m_chunk);
    if (m_chunk != nullptr) {
      continue;
    }
    readChunk(m_chunk);
    while (!m_dstChunkQueuePtr->insert(m_chunk)) {
      usleep(100 * 1000);
    }
  }
  return 0;
}

int FileReader::readChunk(FileChunk *chunk) {
  const long left = chunk->getStartPos();
  const long right = chunk->getEndPos();

  char *memFile = chunk->getMamFile();

  for (long i = left; i <= right; i++) {
    // 开始进行列处理
    long seek = left;
    while (seek <= right) {
      // 从 I D T 开始的每一列序号，第一列为 operation
      int columnNum = 1;
      // 是 B 行的话直接不管
      if (memFile[seek] == 'B') {
        while (memFile[seek] != '\n') { // 注意可能越界
          ++seek;
        }
        ++seek;
        // 可以开始处理这一行 A 和 insert 相同处理即可
        continue;
      }
      dealLine(memFile + seek);
      // TODO ++seek
    }
  }
  return 0;
}

/**
 * 从这里开始处理一行，目的是做一个前置过滤，减少处理量，数据直接拿出来，不做额外处理
 */
void FileReader::dealLine(char *start) {
  char *pos = start;
  char *tmpStart, *tmpEnd, *columnStart;
  // 把一行里面的几个 position，主键搞出来
  OPERATION op = getOpByDesc(*pos);
  pos += 2; // 自己和 \t
  // 处理掉 schema 名
  while (*pos != '\t') {
    ++pos;
  }
  tmpStart = pos;
  while (*pos != '\t') {
    ++pos;
  }
  tmpEnd = pos;
  string tableName(tmpStart, tmpEnd - tmpStart);
  // 根据不同的 table 有不同的处理方式
  TABLE_ID tableId = getTableIdByName(tableName);
  auto table = g_tableMap[tableName];
  ++pos; // \t
  columnStart = pos; // 列的真实开始位置
  int columnIndex = 0;
  int ids[4];
  int timeSeek = -1;
  while (*pos != '\n') {
    tmpStart = pos;
    while (*pos != '\t' && *pos != '\n') {
      ++pos;
    }
    tmpEnd = pos;
    if (table->columns[columnIndex].isPk) {
      ids[table->columns[columnIndex].pkOrder] = atoi(tmpStart);
    } else if (table->columns[columnIndex].getDataType() == MYSQL_TYPE_DATETIME) {
      timeSeek = tmpStart - columnStart; // 记录一下对应的 time 所在的位置，每个表只有一个列，所以这就记录一次
    }
    ++columnIndex;
  }
  // 读完了这一行，看看是否需要放到chunk的栈里面
  // 提前检验一次 pk 是否已经被处理过
  if (g_bitmapManager->checkExistsNoLock(tableId, ids)) { // 存在的话直接返回不需要处理了
    return;
  }

  auto *line = new LineRecord();

  //设置本行 line 对应的 TableID
  line->table = tableId;
  //设置本行line对应的 chunkId 信息
  line->chunkId = m_chunk->getChunkNo();
  //设置本行 line 的起始位置地址
  line->memFile = start;
  // 设置这一行的数据有多长
  line->size = pos - start;
  // 设置时间字段的长度信息
  line->datetimeStartPos = timeSeek;

  //本 chunk 的 line 解析信息记录
  m_chunk->addLine(line);

}
