//
// Created by lesss on 2021/8/7.
//

//
#include <pthread.h>
#include <string>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>

//自有文件头
#include "file_split.h"
#include "file_util.h"
#include "common.h"

using namespace std;

int FileSplitter::init()
{
    return 0;
}

void *FileSplitter::start(void *args)
{
    // TODO 这里的队列需要改造处理，这里应该类似 golang 的 chan 进行操作处理，没有的话，死等
    for (auto name : this->waitDealFiles)
    {
        // 开始进行文件 mmap 映射
        char *mem_file = (char *)file_mmap(name.c_str());
        if (mem_file == nullptr)
        {
            exit(1);
        }

        int chunkNo = 0;

        long startPos = 0;
        for (long i = 0; i < sizeof(*mem_file); i++)
        {
            long endPos = PerChunkSize;
            const char *a = &mem_file[endPos];
            if (!strcmp(a, "\n"))
            {
                // TODO 当前位置不是换行符号，左右同时搜索
                int tmpLeft = endPos - 1;
                int tmpRight = endPos + 1;
                for (;;)
                {
                    const char *l = &mem_file[tmpLeft];
                    if (strcmp(l, "\n"))
                    {
                        endPos = tmpLeft;
                        break;
                    }
                    const char *r = &mem_file[tmpRight];
                    if (strcmp(r, "\n"))
                    {
                        endPos = tmpRight;
                        break;
                    }

                    tmpLeft--;
                    tmpRight++;
                }
            }

            //TODO 找到了换行符号，创建一个对应的 FileChunk，发送给下一个流程进行处理
            FileChunk *chunk = &FileChunk("", chunkNo, mem_file, startPos, endPos - 1);
            chunkNo++;
            // 设置下一个位置的起始
            startPos = endPos + 1;

            // TODO 将 FileChunk 推送到消息队列中进行处理
        }

        return nullptr;
    }
