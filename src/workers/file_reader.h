//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_FILE_READER_H
#define HIGHDTS_FILE_READER_H


#include "common.h"

class FileReader {

public:

    /**
     * 开始读取文件块
     * @param fileChunk
     * @return
     */
    int start_read_chunk(FileChunk *fileChunk);

    /**
     * 按行读取每条记录
     * @return
     */
    struct LineRecord* read_line();

    void set_cur_chunk(struct FileChunk* chunk) {
        curChunk = chunk;
    }

private:


    /**
     * 当前的 FileReader 处理的 FileChunk
     */
    struct FileChunk* curChunk;

};


#endif //HIGHDTS_FILE_READER_H
