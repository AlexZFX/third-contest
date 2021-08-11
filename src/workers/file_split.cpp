//
// Created by lesss on 2021/8/7.
//

//
#include <pthread.h>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>

//自有文件头
#include "global_obj.hpp"
#include "file_split.h"
#include "../common/FileChunk.h"
#include "../common/Common.h"

using namespace std;

int FileSplitter::init()
{
    return 0;
}

void *FileSplitter::start(void *args)
{

    for (auto name : this->waitDealFiles)
    {
        int fd = open(name.c_str(), O_RDWR | O_CREAT, 0666);
        if (fd == -1)
        {
            cout << "[ERROR] file can't open : " + name << endl;
            return nullptr;
        }

        struct stat st;
        int ret = fstat(fd, &st);

        // 开始进行文件 mmap 映射
        char *mem_file = (char *)mmap(NULL, st.st_size, PROT_READ, MAP_FILE, fd, 0);

        close(fd);

        if (mem_file == MAP_FAILED)
        {
            // TODO 这里 mmap 失败，是否直接退出
            cout << "[ERROR] do mmap failed " << endl;
            return nullptr;
        }

        int chunkNo = 0;

        for (long i = 0; i < st.st_size; i++)
        {
            long startPos = i;
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

            //TODO 找打了换行符号
            FileChunk *chunk = &FileChunk(chunkNo, mem_file, startPos, endPos - 1, st.st_size, i == st.st_size - 1);

            // 发送到对应的队列中去
            ChunkMQ.Push(chunk);

            chunkNo++;
            // 设置下一个位置的起始
            i = endPos;
        }

        return nullptr;
    }
}

class FileChunkManager
{
private:
    map<string, *FileChunk> _chunkRepository;

public:
    FileChunkManager(/* args */);
    ~FileChunkManager();
};

FileChunkManager::FileChunkManager(/* args */)
{
}

FileChunkManager::~FileChunkManager()
{
}