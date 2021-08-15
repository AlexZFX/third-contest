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
    auto tableId = static_cast<TABLE_ID>(stoi(tableName));
    switch (tableId) {
      case TABLE_WAREHOUSE_ID: {
        sprintf(buf, WAREHOUSE_LOAD_SQL.c_str(), tableName.c_str());
        break;
      }
      case TABLE_ORDERS_LINE_ID: {
        sprintf(buf, ORDER_LINE_LOAD_SQL.c_str(), tableName.c_str());
        break;
      }
      case TABLE_DISTRICT_ID: {
        sprintf(buf, DISTRICT_LOAD_SQL.c_str(), tableName.c_str());
        break;
      }
      case TABLE_CUSTOMER_ID: {
        sprintf(buf, CUSTOMER_LOAD_SQL.c_str(), tableName.c_str());
        break;
      }
      case TABLE_NEW_ORDERS_ID: {
        sprintf(buf, NEW_ORDERS_LOAD_SQL.c_str(), tableName.c_str());
        break;
      }
      case TABLE_ORDERS_ID: {
        sprintf(buf, ORDERS_LOAD_SQL.c_str(), tableName.c_str());
        break;
      }
      case TABLE_ITEM_ID: {
        sprintf(buf, ITEM_LOAD_SQL.c_str(), tableName.c_str());
        break;
      }
      case TABLE_STOCK_ID: {
        sprintf(buf, STOCK_LOAD_SQL.c_str(), tableName.c_str());
        break;
      }
      case TABLE_UNKNOWN: {
        LogError("unknown table: %s", tableName.c_str());
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