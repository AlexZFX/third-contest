//
// Created by alexfxzhang on 2021/8/9.
//

#ifndef THIRD_CONTEST_FILECHUNK_H
#define THIRD_CONTEST_FILECHUNK_H

#include <vector>
#include <sys/mman.h>

#include "metadata.h"

/**
 * @brief 文件块表示
 * 
 * chunkNo 的生成方式，文件序号 * 10^6 + (auto_increment)
 */
class FileChunk {

private:
    char *_memFile;
    int _chunkNo;
    bool _isLast;
    long _startPos;
    long _endPos;
    long _memReadPos; //内存中读指针的位置信息
    long _fileSize;
    long _persisReadPos; //元数据中读指针的位置信息，只有对应的行被处理并持久化到对应的load-data文件中之后，触发其更新

public:
    FileChunk(int chunkNo, char *_mem_file, long startPos, long endPos, long fileSize, bool isLast) {
        _chunkNo = chunkNo;
        _startPos = startPos;
        _endPos = endPos;
        _memReadPos = startPos;
        _persisReadPos = _memReadPos;
        _fileSize = fileSize;
        _isLast = isLast;
    }

    /**
     * 更新并保存当前可以被持久化的读指针位置，这里最好使用异步调用的方式
     * @param readPos
     * @return
     */
    int updateAndSavePos(long readPos) {
        _persisReadPos = readPos;
        return 0;
    }

    void release() {
        if (_isLast) {
            munmap(_memFile, _fileSize);
        }
    }

    long getStartPos() {
        return _startPos;
    }

    long getEndPos() {
        return _endPos;
    }

    long getMemReadPos() {
        return _memReadPos;
    }

    char *getMamFile() {
        return _memFile;
    }
};

class LineRecord {
public:
    int operation{};
    string schema;
    string table;
    char uniq[35]{};
    char* fields;

    LineRecord(FileChunk *chunk, long endPos) {
        _chunk = chunk;
        _endPos = endPos;
    }

    ~LineRecord() {
        _chunk->updateAndSavePos(_endPos);
    }

private:
    FileChunk *_chunk;
    long _endPos;
};

#endif //THIRD_CONTEST_FILECHUNK_H
