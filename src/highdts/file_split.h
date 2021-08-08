//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_FILE_SPLIT_H
#define HIGHDTS_FILE_SPLIT_H

#include <string>
#include <mutex>
#include <vector>

using namespace std;

//文件分割者
class FileSplitter {

public:
    FileSplitter(){
        waitDealFiles = vector<string>();
    }

    /**
     * 锁，保护 waitDealFiles
     */
    std::mutex mtx;

    /**
     * 任务ID信息
     */
    pthread_t tid;

    /**
     * 初始化自己的相关信息
     * @return
     */
    int init();

    /**
     * 开始执行文件分割
     * @return
     */
    void* start(void* args);

    /**
     * 添加待分割的文件
     * @param filename
     * @return
     */
    int addFile(const string& filename) {
        this->mtx.lock();
        this->waitDealFiles.push_back(filename);
        this->mtx.unlock();
        return 0;
    }

private:
    vector<string> waitDealFiles;

};

#endif //HIGHDTS_FILE_SPLIT_H
