//
// Created by lesss on 2021/8/7.
//

#include "file_reader.h"
#include <stdio.h>
#include <string.h>

BatchLineRecord *FileReader::read_line()
{
    return nullptr;
}

int FileReader::start_read_chunk(FileChunk *chunk)
{
    const long left = chunk->get_start_pos();
    const long right = chunk->get_end_pos();

    char *mem_file = chunk->get_mam_file();

    for (long i = left; i <= right; i++)
    {
        LineRecord *record = &LineRecord();

        vector<string> tmp = vector<string>();

        // 开始进行列处理
        long seek = left;
        for (; seek <= right; seek++)
        {
            // 字段分割
            if (mem_file[seek] == '\t')
            {
                const int col_size = seek - i;
                char col_arr[col_size];

                // 拷贝对应的数据
                memcpy(col_arr, mem_file + (i), col_size);

                tmp.push_back(string(col_arr));
                continue;
            }

            // 行分割
            if (mem_file[seek] == '\n')
            {
                /* 开始处理这一行的数据 */
                for (auto item : tmp)
                {
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
