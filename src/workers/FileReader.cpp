//
// Created by lesss on 2021/8/7.
//

#include <stdio.h>
#include <string.h>
#include <vector>

#include "entity/Table.h"
#include "entity/Column.h"
#include "FileReader.h"

BatchLineRecord *FileReader::readLines() {
  return nullptr;
}

int FileReader::startReadChunk(FileChunk *chunk) {
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
        const int col_size = seek - i;
        char col_arr[col_size];

        // 拷贝对应的数据
        memcpy(col_arr, mem_file + (i), col_size);

        tmp.push_back(string(col_arr));
        continue;
      }

      // 行分割
      if (mem_file[seek] == '\n') {
        // 创建一个 LineRecord 对象
        LineRecord *record = &LineRecord(chunk, seek);

        /* 开始处理这一行的数据 */

        record->operation = getOpByDesc(tmp[0]);
        record->schema = tmp[1];
        record->table = tmp[2];
        record->field = vector<string>(tmp.size() - 3);

        // 找到对应的 schema、table 元数据标识
        const Table *table = getTableDesc(record->table);
        const vector<int> pkPos = table->getPkOrders();

        // 平凑出 pk-uk 的组装数据
        string uniq = "";
        for (int pos : pkPos) {
          uniq += tmp[3 + pos];
        }

        record->uniq = uniq.c_str();

        for (int i = 3; i < tmp.size(); i++) {
          record->field[i - 3] = tmp[i];
        }

        // 清理临时数组
        tmp.clear();

        // 发送到下一个通道

        // 跳出本次的行处理操作
        i = seek + 1;
        break;
      }
    }
  }
  return 0;
}
