//
// Created by lesss on 2021/8/7.
//

#include "LoadDataWorker.h"
#include "../utils/logger.h"
#include "../common/DtsConf.h"
#include <cstdlib>
#include <unistd.h>
#include "common/Common.h"

extern DtsConf g_conf;


using namespace std;

bool LoadDataWorker::init() {
  m_mysql = new CNewMysql();
  string url = g_conf.outputDbUrl.substr(0, g_conf.outputDbUrl.find_last_of(':'));
  string database = g_conf.outputDbUrl.substr(g_conf.outputDbUrl.find_last_of('/') + 1);
  int port = atoi(g_conf.outputDbUrl.substr(g_conf.outputDbUrl.find_last_of(':') + 1,
                                            g_conf.outputDbUrl.find_last_of('/') -
                                            g_conf.outputDbUrl.find_last_of(':') - 1).c_str());
  if (m_mysql->init(url, port, g_conf.outputDbUser, g_conf.outputDbPass, 3600, database, "utf8mb4")) {
    return true;
  } else {
    cerr << m_mysql->getErr() << endl;
    return false;
  }
}


int LoadDataWorker::run() {
  while (m_threadstate) {
    string fileName = "";
    m_queuePtr->dequeue(1, fileName);
    if (fileName.empty()) {
      if (g_conf.loadFileWriteFinish) {
        LogInfo("load data worker load finished, will exit");
        return 0;
      }
      LogInfo("get empty file name, will continue and retry");
      continue;
    }
    string tableName = fileName.substr(fileName.find_last_of(SLASH_SEPARATOR) + 1,
                                       fileName.find_last_of('_') - fileName.find_last_of(SLASH_SEPARATOR) - 1);
    auto tableId = static_cast<TABLE_ID>(stoi(tableName));

    char buf[1024] = {0};
    switch (tableId) {
      case TABLE_ID::TABLE_WAREHOUSE_ID: {
        sprintf(buf, WAREHOUSE_LOAD_SQL.c_str(), fileName.c_str());
        break;
      }
      case TABLE_ID::TABLE_ORDERS_LINE_ID: {
        sprintf(buf, ORDER_LINE_LOAD_SQL.c_str(), fileName.c_str());
        break;
      }
      case TABLE_ID::TABLE_DISTRICT_ID: {
        sprintf(buf, DISTRICT_LOAD_SQL.c_str(), fileName.c_str());
        break;
      }
      case TABLE_ID::TABLE_CUSTOMER_ID: {
        sprintf(buf, CUSTOMER_LOAD_SQL.c_str(), fileName.c_str());
        break;
      }
      case TABLE_ID::TABLE_NEW_ORDERS_ID: {
        sprintf(buf, NEW_ORDERS_LOAD_SQL.c_str(), fileName.c_str());
        break;
      }
      case TABLE_ID::TABLE_ORDERS_ID: {
        sprintf(buf, ORDERS_LOAD_SQL.c_str(), fileName.c_str());
        break;
      }
      case TABLE_ID::TABLE_ITEM_ID: {
        sprintf(buf, ITEM_LOAD_SQL.c_str(), fileName.c_str());
        break;
      }
      case TABLE_ID::TABLE_STOCK_ID: {
        sprintf(buf, STOCK_LOAD_SQL.c_str(), fileName.c_str());
        break;
      }
      case TABLE_ID::TABLE_UNKNOWN: {
        LogError("unknown table: %s", fileName.c_str());
        break;
      }
    }
    CSelectResult result;
    while (m_mysql->query(buf, result) < 0) {
      LogError("load data failed , error: %s, sleep 1 and retry", m_mysql->getErr());
      usleep(100 * 1000);
    }
    // TODO metadata
  }
  return 0;
}