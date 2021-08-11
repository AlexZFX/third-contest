//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_LOADDATA_WORKER_H
#define HIGHDTS_LOADDATA_WORKER_H

#include "../utils/BaseThread.h"
#include "../utils/ThreadSafaQueue.h"
#include "../utils/newmysql.h"

class LoadDataWorker : public BaseThread {

private:

  ThreadSafeQueue<std::string> *m_queuePtr;

  CNewMysql *m_mysql;

public:
  LoadDataWorker(ThreadSafeQueue<std::string> *queuePtr) : m_queuePtr(queuePtr), m_mysql(nullptr) {};

  bool init();

  int run();

  ~LoadDataWorker() {
    delete m_mysql;
  }


};


#endif //HIGHDTS_LOADDATA_WORKER_H
