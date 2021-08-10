//
// Created by alexfxzhang on 2021/3/8.
//

#ifndef THIRD_CONTEST_BASETHREAD_H
#define THIRD_CONTEST_BASETHREAD_H

#include "pthread.h"

class BaseThread {
public:
  BaseThread();

  ~BaseThread();

  int start();

  pthread_t getThreadID() {
    return m_threadID;
  }

  //只设置线程为终止状态，不强行退出
  virtual int stop();

  void join() {
    if (m_threadID != nullptr) {
      pthread_join(m_threadID, nullptr);
      m_threadID = nullptr;
    }
  }

protected:
  virtual int run() = 0;    //请定义子类的执行函数
  int m_threadstate;    //0-线程退出 1-线程运行

private:
  int initThread();        //不能重载
  static void *startThread(void *);

private:
  pthread_t m_threadID;
  pthread_attr_t m_attr{};
};

#endif //THIRD_CONTEST_BASETHREAD_H