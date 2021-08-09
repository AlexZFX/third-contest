//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_COMMON_H
#define HIGHDTS_COMMON_H

#include <string>
#include <utility>
#include <vector>
#include "metadata.h"

using namespace std;

/**
 * @brief 按照每个chunk 16M 进行处理
 * 
 */
const long PerChunkSize = 16 * 1024 * 1024;

/**
 * 文件块表示
 */
class FileChunk : public Metadata
{

private:
    char *_mem_file;
    int _chunkNo;
    long _startPos;
    long _endPos;
    long _memReadPos;    //内存中读指针的位置信息
    long _persisReadPos; //元数据中读指针的位置信息，只有对应的行被处理并持久化到对应的load-data文件中之后，触发其更新

public:
    FileChunk(string savePath, int chunkNo, char *_mem_file, long startPos, long endPos) : Metadata(std::move(savePath))
    {
        _chunkNo = chunkNo;
        _startPos = startPos;
        _endPos = endPos;
        _memReadPos = startPos;
        _persisReadPos = _memReadPos;
    }

    int update_and_save_pos(long readPos);

    long get_start_pos()
    {
        return _startPos;
    }

    long get_end_pos()
    {
        return _endPos;
    }

    long get_mem_read_pos()
    {
        return _memReadPos;
    }

    char *get_mam_file()
    {
        return _mem_file;
    }
};

/**
 * 更新并保存当前可以被持久化的读指针位置，这里最好使用异步调用的方式
 * @param readPos
 * @return
 */
int FileChunk::update_and_save_pos(long readPos)
{
    _persisReadPos = readPos;

    return persistent();
}

// 批量记录
struct BatchLineRecord
{
    LineRecord records[];
};

struct LineRecord
{
    int operation;
    string schema;
    string table;
    char uniq[35];
    vector<string> field;
};

#endif //HIGHDTS_COMMON_H
