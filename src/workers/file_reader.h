//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_FILE_READER_H
#define HIGHDTS_FILE_READER_H

#include "../common/Common.h"
#include "../common/FileChunk.h"

class FileReader {

public:
    /**
     * 开始读取文件块
     * @param chunk
     * @return
     */
    int startReadChunk(FileChunk *chunk);

    void setCurChunk(FileChunk *chunk) {
        _curChunk = chunk;
    }

private:
    /**
     * 当前的 FileReader 处理的 FileChunk
     */
    struct FileChunk *_curChunk;
};

#endif //HIGHDTS_FILE_READER_H
