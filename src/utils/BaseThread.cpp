//
// Created by alexfxzhang on 2021/3/9.
//

#include "BaseThread.h"
#include <cstdio>
#include <cstdlib>

BaseThread::BaseThread() {
  m_threadID = 0;
  initThread();
  m_threadstate = 0;
}

BaseThread::~BaseThread() {
  pthread_attr_destroy(&m_attr);
}

int BaseThread::initThread() {
  pthread_attr_init(&m_attr);
  int iRet = 0;
  iRet = pthread_attr_setscope(&m_attr, PTHREAD_SCOPE_SYSTEM);
  if (iRet != 0) {
    //设置失败
    printf("set PTHREAD_SCOPE_SYSTEM error %d\n", iRet);
    pthread_attr_destroy(&m_attr);
    return -1;
  }
  return 0;
}

int BaseThread::start() {
  if (m_threadID != 0) { //已经启动了，不需要重复
    return 0;
  }
  int iRet = 0;
  m_threadstate = 1; //线程运行状态
  if ((iRet = pthread_create(&m_threadID, &m_attr, &startThread, (void *) this)) != 0) {
    printf("create thread error %d\n", iRet);
    pthread_attr_destroy(&m_attr);
    m_threadstate = 0; //线程运行状态
    return -1;
  }
  return 0;
}

void *BaseThread::startThread(void *arg) {
  if (nullptr == arg)
    return nullptr;
  auto *ptr = (BaseThread *) arg;
  ptr->run();

  return nullptr;
}

int BaseThread::stop() {
  m_threadstate = 0;
  return 0;
}