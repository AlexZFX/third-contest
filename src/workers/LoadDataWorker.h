//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_LOADDATA_WORKER_H
#define HIGHDTS_LOADDATA_WORKER_H

#include "../utils/BaseThread.h"
#include "utils/ThreadSafeQueue.h"
#include "../utils/newmysql.h"

class LoadDataWorker : public BaseThread {

private:

  ThreadSafeQueue<std::string> *m_queuePtr;

  CNewMysql *m_mysql;

public:
  explicit LoadDataWorker(ThreadSafeQueue<std::string> *queuePtr) : m_queuePtr(queuePtr), m_mysql(nullptr) {};

  bool init();

  int run() override;

  ~LoadDataWorker() {
    delete m_mysql;
  }
};

class LoadDataWorkerMgn : public BaseThread {
private:
  int m_threadNum;
  LoadDataWorker *workers[100]{};
public:
  LoadDataWorkerMgn(int threadNum, ThreadSafeQueue<string> *queuePtr) : m_threadNum(threadNum) {
    for (int i = 0; i < m_threadNum; ++i) {
      workers[i] = new LoadDataWorker(queuePtr);
      workers[i]->init();
    }
  }

  ~LoadDataWorkerMgn() {
    for (int i = 0; i < m_threadNum; ++i) {
      delete workers[i];
    }
  }

  int run() override {
    for (int i = 0; i < m_threadNum; ++i) {
      workers[i]->start();
    }
    for (int i = 0; i < m_threadNum; ++i) {
      workers[i]->join();
    }
    return 0;
  }
};


#endif //HIGHDTS_LOADDATA_WORKER_H
