//
// Created by lesss on 2021/8/7.
//

#include <stdio.h>
#include <string.h>
#include <vector>

#include "entity/Table.h"
#include "entity/Column.h"
#include "FileReader.h"

int FileReader::run() {
  while (m_threadstate) {
    FileChunk *chunk = nullptr;
    m_chunkQueuePtr->dequeue(1, chunk);
    readChunk(chunk);
  }
  return 0;
}

BatchLineRecord *FileReader::readLines() {
  return nullptr;
}

int FileReader::readChunk(FileChunk *chunk) {
  const long left = chunk->getStartPos();
  const long right = chunk->getEndPos();

  char *mem_file = chunk->getMamFile();

  for (long i = left; i <= right; i++) {
    vector<string> tmp = vector<string>();

    // 开始进行列处理
    long seek = left;
    for (; seek <= right; seek++) {
      // 字段分割
      if (mem_file[seek] == '\t') {
        const int col_size = (int) (seek - i);
        char *col_arr = new char[col_size];

        // 拷贝对应的数据
        memcpy(col_arr, mem_file + (i), col_size);
        string elem(col_arr);
        tmp.push_back(elem);
        continue;
      }

      // 行分割
      if (mem_file[seek] == '\n') {
        // 创建一个 LineRecord 对象
        LineRecord tmpRecord(chunk, seek, seek == right);
        LineRecord *record = &tmpRecord;

        /* 开始处理这一行的数据 */
        record->operation = getOpByDesc(tmp[0]);
        record->schema = tmp[1];
        record->table = tmp[2];

        // 找到对应的 schema、table 元数据标识
        const Table *table = getTableDesc(record->table);
        const vector<Column> columns = table->columns;
        const vector<int> pkPos = table->getPkOrders();

        // 凑出 pk-uk 的组装数据
        long idxs[pkPos.size()];
        for (int pos : pkPos) {
          idxs[pos] = stol(tmp[3 + pos]);
        }
        record->idxs = idxs;

        string tmpFields;
        for (int j = 3; j < tmp.size(); j++) {
          const Column column = columns[j - 3];

          // find datetime field start pos
          if (column.getDataType() == MYSQL_TYPE_DATETIME) {
            record->datetimeStartPos = (int) tmpFields.size() + 1;
          }
          tmpFields += tmp[j];

        }
        record->fields = tmpFields.c_str();

        // 清理临时数组
        tmp.clear();

        // 发送到下一个通道
        LineRecordMQ->enqueue(record);

        // 跳出本次的行处理操作
        i = seek + 1;
        break;
      }
    }
  }
  return 0;
}
