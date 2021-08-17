//
// Created by alexfxzhang on 2021/6/14.
//

#ifndef THIRD_CONTEST_UTIL_H
#define THIRD_CONTEST_UTIL_H

#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <dirent.h>
#include <vector>
#include <string>
#include <algorithm>

#include "../entity/Table.h"
#include "../entity/Column.h"
#include "BitmapManager.hpp"

using namespace std;

extern unordered_map<TABLE_ID, Table *, TABLE_ID_HASH> g_tableMap;

#define SecSnprintf(buf, len, fmt, args...)    snprintf(buf,(len)-1,fmt,##args)
#define SecSprintf(buf, fmt, args...)    snprintf(buf,sizeof((buf))-1,fmt,##args)

inline int64_t getCurrentLocalTimeStamp() {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp = std::chrono::time_point_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now());
  auto tmp = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch());
  return tmp.count();
}

// 获取目录下类型为 REG 的文件名
inline bool getFileNames(const std::string &fileDir, vector<string> &files, const string &prefix = "") {
  DIR *dir;
  struct dirent *ptr;
  if ((dir = opendir(fileDir.c_str())) == nullptr) {
    perror("Open dir error...");
    return false;
  }
  while ((ptr = readdir(dir)) != nullptr) {
    if (ptr->d_type == DT_REG) {
      string fileName = ptr->d_name;
      if (!prefix.empty() && fileName.find_first_of(prefix) == 0) {
        files.emplace_back(fileDir + SLASH_SEPARATOR + fileName);
      } else if (prefix.empty()) {
        files.emplace_back(fileDir + SLASH_SEPARATOR + fileName);
      }
    } else {
      continue;
    }
  }
  closedir(dir);
  sort(files.begin(), files.end());
  return true;
}

// Tokenizes the string using the delimiters.
// Empty tokens will not be included in the result. 优先用这个
inline std::vector<std::string> tokenize(const std::string &s, char delims, size_t num = 0) {
  size_t offset = 0;
  std::vector<std::string> tokens;
  while (true) {
    size_t i = s.find_first_not_of(delims, offset);
    if (std::string::npos == i) {
      break;
    }
    size_t j = s.find_first_of(delims, i);
    if (std::string::npos == j) { //已经到了末尾
      tokens.push_back(s.substr(i));
      offset = s.length();
      break;
    }
    if (tokens.size() + 1 == num) { //已经满足了要求，剩余的全部拷贝
      tokens.push_back(s.substr(i));
      break;
    } else {
      tokens.push_back(s.substr(i, j - i));
      offset = j;
    }
  }
  return tokens;
}

// hard code，写死8张表
inline void initTableMap() {

  /*
customer:大约1500万行
district: 5000行
Item:10万行
   */

  // warehouse
  {
    auto warehouse = new Table{};
    warehouse->table_name = TABLE_WAREHOUSE;
    Column w2{"w_id", 1, MYSQL_TYPE_LONG, 10, 10, 10, true, 0};
    Column w3{"w_name", 2, MYSQL_TYPE_VARCHAR, 10, 10, 10};
    Column w5{"w_street_1", 3, MYSQL_TYPE_VARCHAR, 20, 10, 10};
    Column w6{"w_street_2", 4, MYSQL_TYPE_VARCHAR, 20, 10, 10};
    Column w1{"w_city", 5, MYSQL_TYPE_VARCHAR, 20, 10, 10};
    Column w4{"w_state", 6, MYSQL_TYPE_VARCHAR, 2, 10, 10};
    Column w9{"w_zip", 7, MYSQL_TYPE_VARCHAR, 9, 10, 10};
    Column w7{"w_tax", 8, MYSQL_TYPE_DECIMAL, 4, 2, 10};
    Column w8{"w_ytd", 9, MYSQL_TYPE_DECIMAL, 12, 2, 10};
    warehouse->addColumn(w1);
    warehouse->addColumn(w2);
    warehouse->addColumn(w3);
    warehouse->addColumn(w4);
    warehouse->addColumn(w5);
    warehouse->addColumn(w6);
    warehouse->addColumn(w7);
    warehouse->addColumn(w8);
    warehouse->addColumn(w9);
    warehouse->pksOrd.push_back(0);
//    tableMap["warehouse"] = warehouse;
    g_tableMap[TABLE_ID::TABLE_WAREHOUSE_ID] = warehouse;

    std::vector<Index> indexs = {
      /*w_id*/Index(0, 0, 500, -1)
    };
    g_bitmapManager->registerBitmap(TABLE_ID::TABLE_WAREHOUSE_ID, indexs);

  }
  // district
  {
    auto district = new Table{};
    district->table_name = TABLE_DISTRICT;
    Column d2{"d_id", 1, MYSQL_TYPE_LONG, 10, 10, 10, true, 1};
    Column d9{"d_w_id", 2, MYSQL_TYPE_LONG, 10, 10, 10, true, 0};
    Column d3{"d_name", 3, MYSQL_TYPE_VARCHAR, 10, 10, 10};
    Column d6{"d_street_1", 4, MYSQL_TYPE_VARCHAR, 20, 10, 10};
    Column d7{"d_street_2", 5, MYSQL_TYPE_VARCHAR, 20, 10, 10};
    Column d1{"d_city", 6, MYSQL_TYPE_VARCHAR, 20, 10, 10};
    Column d5{"d_state", 7, MYSQL_TYPE_VARCHAR, 2, 10, 10};
    Column d11{"d_zip", 8, MYSQL_TYPE_VARCHAR, 9, 10, 10};
    Column d8{"d_tax", 9, MYSQL_TYPE_DECIMAL, 4, 2, 10};
    Column d10{"d_ytd", 10, MYSQL_TYPE_DECIMAL, 12, 2, 10};
    Column d4{"d_next_o_id", 11, MYSQL_TYPE_LONG, 12, 2, 10};
    district->addColumn(d1);
    district->addColumn(d2);
    district->addColumn(d3);
    district->addColumn(d4);
    district->addColumn(d5);
    district->addColumn(d6);
    district->addColumn(d7);
    district->addColumn(d8);
    district->addColumn(d9);
    district->addColumn(d10);
    district->addColumn(d11);
    district->pksOrd.push_back(1);
    district->pksOrd.push_back(0);
//    g_tableMap["district"] = district;
    g_tableMap[TABLE_ID::TABLE_DISTRICT_ID] = district;

    /*
     | max(c_w_id) | max(c_d_id) | max(c_id) |
     | 500 | 10 | 3000 |
     */
    std::vector<Index> indexs = {
      /*d_w_id*/Index(0, 0, 500, -1),
      /*d_id*/Index(0, 0, 5000, -1)
    };
//    std::vector<Index> indexs = {
//      /*c_w_id*/Index(0, 0, 500, -1),
//      /*c_d_id*/Index(0, 0, 10, -1),
//      /*c_id*/Index(0, 0, 3000, -1)
//    };
    g_bitmapManager->registerBitmap(TABLE_ID::TABLE_DISTRICT_ID, indexs);
  }
  // customer
  {
    auto customer = new Table{};
    customer->table_name = TABLE_CUSTOMER;
    Column c10{"c_id", 1, MYSQL_TYPE_LONG, 10, 10, 10, true, 2};
    Column c5{"c_d_id", 2, MYSQL_TYPE_LONG, 10, 10, 10, true, 1};
    Column c19{"c_w_id", 3, MYSQL_TYPE_LONG, 10, 10, 10, true, 0};
    Column c9{"c_first", 4, MYSQL_TYPE_VARCHAR, 16, 10, 10};
    Column c12{"c_middle", 5, MYSQL_TYPE_VARCHAR, 2, 10, 10};
    Column c11{"c_last", 6, MYSQL_TYPE_VARCHAR, 16, 10, 10};
    Column c17{"c_street_1", 7, MYSQL_TYPE_VARCHAR, 20, 10, 10};
    Column c18{"c_street_2", 8, MYSQL_TYPE_VARCHAR, 20, 10, 10};
    Column c2{"c_city", 9, MYSQL_TYPE_VARCHAR, 20, 10, 10};
    Column c16{"c_state", 10, MYSQL_TYPE_VARCHAR, 2, 10, 10};
    Column c21{"c_zip", 11, MYSQL_TYPE_VARCHAR, 9, 10, 10};
    Column c14{"c_phone", 12, MYSQL_TYPE_VARCHAR, 16, 10, 10};
    Column c15{"c_since", 13, MYSQL_TYPE_DATETIME, 4, 2, 10};
    Column c3{"c_credit", 14, MYSQL_TYPE_VARCHAR, 2, 10, 10};
    Column c4{"c_credit_lim", 15, MYSQL_TYPE_LONG, 10, 10, 10};
    Column c8{"c_discount", 16, MYSQL_TYPE_DECIMAL, 4, 2, 10};
    Column c1{"c_balance", 17, MYSQL_TYPE_DECIMAL, 12, 2, 10};
    Column c20{"c_ytd_payment", 18, MYSQL_TYPE_DECIMAL, 12, 2, 10};
    Column c13{"c_payment_cnt", 19, MYSQL_TYPE_LONG, 12, 2, 10};
    Column c7{"c_delivery_cnt", 20, MYSQL_TYPE_LONG, 12, 2, 10};
    Column c6{"c_data", 21, MYSQL_TYPE_VAR_STRING, 12, 2, 10};
    customer->addColumn(c1);
    customer->addColumn(c2);
    customer->addColumn(c3);
    customer->addColumn(c4);
    customer->addColumn(c5);
    customer->addColumn(c6);
    customer->addColumn(c7);
    customer->addColumn(c8);
    customer->addColumn(c9);
    customer->addColumn(c10);
    customer->addColumn(c11);
    customer->addColumn(c12);
    customer->addColumn(c13);
    customer->addColumn(c14);
    customer->addColumn(c15);
    customer->addColumn(c16);
    customer->addColumn(c17);
    customer->addColumn(c18);
    customer->addColumn(c19);
    customer->addColumn(c20);
    customer->addColumn(c21);
    customer->pksOrd.push_back(2);
    customer->pksOrd.push_back(1);
    customer->pksOrd.push_back(0);
//    g_tableMap["customer"] = customer;
    g_tableMap[TABLE_ID::TABLE_CUSTOMER_ID] = customer;

    /*
        | max(c_w_id) | max(c_d_id) | max(c_id)
        | 500 | 10 | 3000 |
     */

    std::vector<Index> indexs = {
      /*c_w_id*/ Index(2, 0, 500, -1),
      /*c_d_id*/ Index(1, 0, 10, -1),
      /*c_id*/ Index(0, 0, 5000, -1)
    };
    g_bitmapManager->registerBitmap(TABLE_ID::TABLE_CUSTOMER_ID, indexs);
  }
  // new_orders
  {
    auto new_orders = new Table{};
    new_orders->table_name = TABLE_NEW_ORDERS;
    Column w2{"no_o_id", 1, MYSQL_TYPE_LONG, 10, 10, 10, true, 2};
    Column w1{"no_d_id", 2, MYSQL_TYPE_LONG, 10, 10, 10, true, 1};
    Column w3{"no_w_id", 3, MYSQL_TYPE_LONG, 20, 10, 10, true, 0};
    new_orders->addColumn(w1);
    new_orders->addColumn(w2);
    new_orders->addColumn(w3);
    new_orders->pksOrd.push_back(2);
    new_orders->pksOrd.push_back(1);
    new_orders->pksOrd.push_back(0);
//    g_tableMap["new_orders"] = new_orders;
    g_tableMap[TABLE_ID::TABLE_NEW_ORDERS_ID] = new_orders;

    std::vector<Index> indexs = {
      /*no_w_id*/Index(2, 0, 500, -1),
      /*no_d_id*/Index(1, 0, 10, -1),
      /*no_o_id*/Index(0, 0, 5000, -1)
    };
    g_bitmapManager->registerBitmap(TABLE_ID::TABLE_NEW_ORDERS_ID, indexs);
  }
  // orders
  {
    auto orders = new Table{};
    orders->table_name = TABLE_ORDERS;
    Column w6{"o_id", 1, MYSQL_TYPE_LONG, 10, 10, 10, true, 2};
    Column w4{"no_d_id", 2, MYSQL_TYPE_LONG, 10, 10, 10, true, 1};
    Column w8{"no_w_id", 3, MYSQL_TYPE_LONG, 20, 10, 10, true, 0};
    Column w2{"o_c_id", 4, MYSQL_TYPE_LONG, 20, 10, 10};
    Column w5{"o_entry_d", 5, MYSQL_TYPE_DATETIME, 20, 10, 10};
    Column w3{"o_carrier_id", 6, MYSQL_TYPE_LONG, 2, 10, 10};
    Column w7{"o_ol_cnt", 7, MYSQL_TYPE_LONG, 9, 10, 10};
    Column w1{"o_all_local", 8, MYSQL_TYPE_LONG, 4, 2, 10};
    orders->addColumn(w1);
    orders->addColumn(w2);
    orders->addColumn(w3);
    orders->addColumn(w4);
    orders->addColumn(w5);
    orders->addColumn(w6);
    orders->addColumn(w7);
    orders->addColumn(w8);
    orders->pksOrd.push_back(2);
    orders->pksOrd.push_back(1);
    orders->pksOrd.push_back(0);
//    g_tableMap["orders"] = orders;
    g_tableMap[TABLE_ID::TABLE_ORDERS_ID] = orders;

    std::vector<Index> indexs = {
      /*no_w_id*/Index(2, 0, 500, -1),
      /*no_d_id*/Index(1, 0, 10, -1),
      /*o_id*/Index(0, 0, 5000, -1)
    };
    g_bitmapManager->registerBitmap(TABLE_ID::TABLE_ORDERS_ID, indexs);
  }
  // order_line
  {
    auto order_line = new Table{};
    order_line->table_name = TABLE_ORDERS_LINE;
    Column w7{"ol_o_id", 1, MYSQL_TYPE_LONG, 10, 10, 10, true, 2};
    Column w2{"ol_d_id", 2, MYSQL_TYPE_LONG, 10, 10, 10, true, 1};
    Column w10{"ol_w_id", 3, MYSQL_TYPE_LONG, 20, 10, 10, true};
    Column w6{"ol_number", 4, MYSQL_TYPE_LONG, 20, 10, 10, true, 3};
    Column w5{"ol_i_id", 5, MYSQL_TYPE_LONG, 20, 10, 10};
    Column w9{"ol_supply_w_id", 6, MYSQL_TYPE_LONG, 2, 10, 10};
    Column w3{"ol_delivery_d", 7, MYSQL_TYPE_DATETIME, 9, 10, 10};
    Column w8{"ol_quantity", 8, MYSQL_TYPE_LONG, 4, 2, 10};
    Column w1{"ol_amount", 9, MYSQL_TYPE_DECIMAL, 6, 2, 10};
    Column w4{"ol_dist_info", 10, MYSQL_TYPE_VARCHAR, 24, 2, 10};
    order_line->addColumn(w1);
    order_line->addColumn(w2);
    order_line->addColumn(w3);
    order_line->addColumn(w4);
    order_line->addColumn(w5);
    order_line->addColumn(w6);
    order_line->addColumn(w7);
    order_line->addColumn(w8);
    order_line->addColumn(w9);
    order_line->addColumn(w10);
    order_line->pksOrd.push_back(2);
    order_line->pksOrd.push_back(1);
    order_line->pksOrd.push_back(0);
    order_line->pksOrd.push_back(3);
//    g_tableMap["order_line"] = order_line;
    g_tableMap[TABLE_ID::TABLE_ORDERS_LINE_ID] = order_line;

    /*
        | max(ol_w_id) | max(ol_d_id) | max(ol_o_id) | max(ol_number) |
        | 500 | 10 | 3000 | 15 |
     */

    std::vector<Index> indexs = {
      /*ol_w_id*/Index(2, 0, 500, -1),
      /*ol_d_id*/Index(1, 0, 10, -1),
      /*ol_o_id*/Index(0, 0, 5000, -1),
      /*ol_number*/Index(3, 0, 15, -1)
    };
    g_bitmapManager->registerBitmap(TABLE_ID::TABLE_ORDERS_LINE_ID, indexs);
  }
  // item
  {
    auto item = new Table{};
    item->table_name = TABLE_ITEM;
    Column w2{"i_id", 1, MYSQL_TYPE_LONG, 10, 10, 10, true};
    Column w3{"i_im_id", 2, MYSQL_TYPE_LONG, 10, 10, 10};
    Column w4{"i_name", 3, MYSQL_TYPE_VARCHAR, 24, 10, 10};
    Column w5{"i_price", 4, MYSQL_TYPE_DECIMAL, 5, 2, 10};
    Column w1{"i_data", 5, MYSQL_TYPE_VARCHAR, 50, 10, 10};
    item->addColumn(w1);
    item->addColumn(w2);
    item->addColumn(w3);
    item->addColumn(w4);
    item->addColumn(w5);

    item->pksOrd.push_back(0);
//    g_tableMap["item"] = item;
    g_tableMap[TABLE_ID::TABLE_ITEM_ID] = item;

    /*
     | max(i_id) |
     | 100000 |
     */

    std::vector<Index> indexs = {/*i_id*/Index(0, 0, 100000, -1)};
    g_bitmapManager->registerBitmap(TABLE_ID::TABLE_ITEM_ID, indexs);
  }
  // stock
  {
    auto stock = new Table{};
    stock->table_name = TABLE_STOCK;
    Column w12{"s_i_id", 1, MYSQL_TYPE_LONG, 10, 10, 10, true, 1};
    Column w16{"s_w_id", 2, MYSQL_TYPE_LONG, 10, 10, 10, true};
    Column w14{"s_quantity", 3, MYSQL_TYPE_LONG, 20, 10, 10};
    Column w2{"s_dist_01", 4, MYSQL_TYPE_VARCHAR, 24, 10, 10};
    Column w3{"s_dist_02", 5, MYSQL_TYPE_VARCHAR, 24, 10, 10};
    Column w4{"s_dist_03", 6, MYSQL_TYPE_VARCHAR, 24, 10, 10};
    Column w5{"s_dist_04", 7, MYSQL_TYPE_VARCHAR, 24, 10, 10};
    Column w6{"s_dist_05", 8, MYSQL_TYPE_VARCHAR, 24, 2, 10};
    Column w7{"s_dist_06", 9, MYSQL_TYPE_VARCHAR, 24, 2, 10};
    Column w8{"s_dist_07", 10, MYSQL_TYPE_VARCHAR, 24, 2, 10};
    Column w9{"s_dist_08", 11, MYSQL_TYPE_VARCHAR, 24, 2, 10};
    Column w10{"s_dist_09", 12, MYSQL_TYPE_VARCHAR, 24, 2, 10};
    Column w11{"s_dist_10", 13, MYSQL_TYPE_VARCHAR, 24, 2, 10};
    Column w17{"s_ytd", 14, MYSQL_TYPE_DECIMAL, 8, 0, 10};
    Column w13{"s_order_cnt", 15, MYSQL_TYPE_LONG, 24, 2, 10};
    Column w15{"s_remote_cnt", 16, MYSQL_TYPE_LONG, 24, 2, 10};
    Column w1{"s_data", 17, MYSQL_TYPE_VARCHAR, 50, 2, 10};
    stock->addColumn(w1);
    stock->addColumn(w2);
    stock->addColumn(w3);
    stock->addColumn(w4);
    stock->addColumn(w5);
    stock->addColumn(w6);
    stock->addColumn(w7);
    stock->addColumn(w8);
    stock->addColumn(w9);
    stock->addColumn(w10);
    stock->addColumn(w11);
    stock->addColumn(w12);
    stock->addColumn(w13);
    stock->addColumn(w14);
    stock->addColumn(w15);
    stock->addColumn(w16);
    stock->addColumn(w17);
    stock->pksOrd.push_back(1);
    stock->pksOrd.push_back(0);
    g_tableMap[TABLE_ID::TABLE_STOCK_ID] = stock;

    std::vector<Index> indexs = {
      /*s_w_id*/Index(1, 0, 500, -1),
      /*s_i_id*/Index(0, 0, 100000, -1)
    };
    g_bitmapManager->registerBitmap(TABLE_ID::TABLE_STOCK_ID, indexs);
  }
}

#endif //THIRD_CONTEST_UTIL_H
