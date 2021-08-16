//
// Created by alexfxzhang on 2021/8/12.
//

#ifndef THIRD_CONTEST_LOADFILEWRITER_H
#define THIRD_CONTEST_LOADFILEWRITER_H

#include <utility>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

#include "../lib/boost/lockfree/spsc_queue.hpp"
#include "../utils/BaseThread.h"
#include "../common/FileChunk.h"
#include "../utils/Util.h"
#include "../common/Common.h"
#include "../utils/ThreadSafeQueue.h"

using namespace std;

/**
 * 将行写到待loaddata处理的文件中
 *
 * 初步先实现成文件线性处理，后续可以优化成多线程并发
 */
class LoadFileWriter : public BaseThread {
private:
    int currentChunkId;
    string curFileName;
    string tableName;
    int fileIndex;
    int32_t maxFileSize;
    int32_t size;
    char *fileStartPtr;
    char *curFilePtr;

    ThreadSafeQueue<LineRecord *> *lineQueue;
    ThreadSafeQueue<std::string> *dstFileQueue;

public:

    LoadFileWriter(string table, ThreadSafeQueue<std::string> *queuePtr, ThreadSafeQueue<LineRecord *> *lineQueuePtr)
            : tableName(std::move(table)), fileIndex(0),
              maxFileSize(LoadFileSize), size(0),
              dstFileQueue(queuePtr),
              lineQueue(lineQueuePtr) {
        curFileName = tableName + "_" + to_string(fileIndex);
        int fd = open(curFileName.c_str(), O_WRONLY);
        fileStartPtr = static_cast<char *>(mmap(nullptr, maxFileSize, PROT_WRITE, MAP_SHARED, fd, 0));
        close(fd);
        curFilePtr = fileStartPtr;
    }

    int run() {
        while (m_threadstate) {
            LineRecord *line = lineQueue->dequeue();
            if (size + line->size > maxFileSize) {
                munmap(fileStartPtr, size);
                // 文件进队，待 load
                dstFileQueue->enqueue(curFileName);
                fileIndex++;
                curFileName = tableName + "_" + to_string(fileIndex);
                int fd = open(curFileName.c_str(), O_WRONLY);
                fileStartPtr = static_cast<char *>(mmap(nullptr, maxFileSize, PROT_WRITE, MAP_SHARED, fd, 0));
                close(fd);
            }
            memcpy(curFilePtr + size, line->memFile, size);
        }

    };

    // 把一个处理好的line写入到文件里面
    //TODO 这里存在一个问题，最后一个数据有可能无法满足 size 的条件进而导致存在残留数据无法处理
    bool write(LineRecord *line) {
        lineQueue->enqueue(line);
    }

};

class LoadFileWriterMgn : public BaseThread {
private:
    map<string, LoadFileWriter *> workers;
public:
    LoadFileWriterMgn(ThreadSafeQueue<std::string> *queuePtr) {
        for (const auto &item : g_tableMap) {
            workers[item.second->getTableName()] = new LoadFileWriter(item.second->getTableName(), queuePtr,
                                                                      new ThreadSafeQueue<LineRecord *>());
        }
    }

    ~LoadFileWriterMgn() {
        for (auto &worker : workers) {
            delete worker.second;
        }
    }

    int run() override {
        for (auto &worker : workers) {
            worker.second->start();
        }
        for (auto &worker : workers) {
            worker.second->join();
        }
    }

    void doWrite(const string &table, LineRecord *record) {
        LoadFileWriter *writer = workers[table];
        writer->write(record);
    }
};

#endif //THIRD_CONTEST_LOADFILEWRITER_H
