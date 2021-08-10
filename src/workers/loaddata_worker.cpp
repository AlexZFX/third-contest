//
// Created by lesss on 2021/8/7.
//

#include "loaddata_worker.h"
#include "../utils/logger.h"
#include "../common/DtsConf.h"
#include <cstdlib>
#include <unistd.h>

extern DtsConf g_conf;


using namespace std;

bool LoadDataWorker::init() {
  m_mysql = new CNewMysql();
  string url = g_conf.outputDbUrl.substr(0, g_conf.outputDbUrl.find_last_of(':'));
  string database = g_conf.outputDbUrl.substr(g_conf.outputDbUrl.find_last_of('/') + 1);
  int port = atoi(g_conf.outputDbUrl.substr(g_conf.outputDbUrl.find_last_of(':') + 1,
                                            g_conf.outputDbUrl.find_last_of('/') -
                                            g_conf.outputDbUrl.find_last_of(':') - 1).c_str());
  m_mysql->init(url, port, g_conf.outputDbUser, g_conf.outputDbPass, 3600, database, "utf8mb4");
}


int LoadDataWorker::run() {
  while (m_threadstate) {
    string fileName;
    m_queuePtr->dequeue(1000, fileName);
    if (fileName.empty()) {
      LogInfo("get empty file name, will continue and retry");
      continue;
    }
    char buf[1024] = {0};
    string tableName = fileName.substr(0, fileName.find_first_of('_'));
    sprintf(buf, "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE "
                 "FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'"
                 "LINES TERMINATED BY '\\n' STARTING BY ''", tableName.c_str());
    CSelectResult result;
    while (m_mysql->query(buf, result) < 0) {
      LogError("load data failed , error: %s, sleep 1 and retry", m_mysql->getErr());
      usleep(100 * 1000);
    }
  }
  return 0;
}