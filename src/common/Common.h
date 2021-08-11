//
// Created by alexfxzhang on 2021/6/12.
//

#ifndef THIRD_CONTEST_COMMON_H
#define THIRD_CONTEST_COMMON_H

#include <string>
#include <unistd.h>

/**
 * @brief 按照每个chunk 16M 进行处理
 *
 */
const long PerChunkSize = 16 * 1024 * 1024;

const std::string DATABASE_NAME = "tianchi_dts_data";                                                       // 待处理数据库名，无需修改
const std::string SCHEMA_FILE_DIR = "schema_info_dir";                                                      // schema文件夹，无需修改。
const std::string SCHEMA_FILE_NAME = "schema.info";                                                         // schema文件名，无需修改。
const std::string SOURCE_FILE_DIR = "source_file_dir";                                                      // 输入文件夹，无需修改。
const std::string SINK_FILE_DIR = "sink_file_dir";                                                          // 输出文件夹，无需修改。
const std::string SOURCE_FILE_NAME_TEMPLATE = "tianchi_dts_source_data_";                                   // 输入文件名，无需修改。
const std::string SINK_FILE_NAME_TEMPLATE = "tianchi_dts_sink_data_";                                       // 输出文件名模板，无需修改。
const std::string CHECK_TABLE_SETS = "customer,district,item,new_orders,order_line,orders,stock,warehouse"; // 待处理表集合，无需修改。
const std::vector<std::string> CHECK_TABLE_LIST = {"customer", "district", "item", "new_orders", "order_line", "orders",
                                                   "stock", "warehouse"};

const std::string TABLE_WAREHOUSE = "warehouse";
const std::string TABLE_DISTRICT = "district";
const std::string TABLE_CUSTOMER = "customer";
const std::string TABLE_NEW_ORDERS = "new_orders";
const std::string TABLE_ORDERS = "orders";
const std::string TABLE_ORDERS_LINE = "order_line";
const std::string TABLE_ITEM = "item";
const std::string TABLE_STOCK = "stock";

const std::string SLASH_SEPARATOR = "/";

const std::string DATE_REGEX_STR = ".*[a-zA-Z]+.*";
const std::string DECIMAL_2_FORMAT = "%.2lf";
const std::string DECIMAL_0_FORMAT = "%.0lf";

const std::string INSERT_OPERATION_DESC = "I";
const std::string DELETE_OPERATION_DESC = "D";
const std::string BEFORE_DATE_IMG_DESC = "A";
const std::string AFTER_DATE_IMG_DESC = "B";

const int INSERT_OPERATION = 1;
const int DELETE_OPERATION = -1;
const int BEFORE_DATE_IMG = 4;
const int AFTER_DATE_IMG = 5;

/**
 * @brief Get the Op By Desc object
 * 
 * @param opDesc 
 * @return const int 
 */
const int getOpByDesc(string opDesc)
{
  switch (opDesc)
  {
  case INSERT_OPERATION_DESC:
    return INSERT_OPERATION;
  case DELETE_OPERATION_DESC:
    return DELETE_OPERATION;
  case BEFORE_DATE_IMG_DESC:
    return BEFORE_DATE_IMG;
  case AFTER_DATE_IMG_DESC:
    return AFTER_DATE_IMG;
  }
}

typedef enum enum_field_types
{
  MYSQL_TYPE_DECIMAL,
  MYSQL_TYPE_TINY,
  MYSQL_TYPE_SHORT,
  MYSQL_TYPE_LONG,
  MYSQL_TYPE_FLOAT,
  MYSQL_TYPE_DOUBLE,
  MYSQL_TYPE_NULL,
  MYSQL_TYPE_TIMESTAMP,
  MYSQL_TYPE_LONGLONG,
  MYSQL_TYPE_INT24,
  MYSQL_TYPE_DATE,
  MYSQL_TYPE_TIME,
  MYSQL_TYPE_DATETIME,
  MYSQL_TYPE_YEAR,
  MYSQL_TYPE_NEWDATE,
  MYSQL_TYPE_VARCHAR,
  MYSQL_TYPE_BIT,
  MYSQL_TYPE_TIMESTAMP2,
  MYSQL_TYPE_DATETIME2,
  MYSQL_TYPE_TIME2,
  MYSQL_TYPE_JSON = 245,
  MYSQL_TYPE_NEWDECIMAL = 246,
  MYSQL_TYPE_ENUM = 247,
  MYSQL_TYPE_SET = 248,
  MYSQL_TYPE_TINY_BLOB = 249,
  MYSQL_TYPE_MEDIUM_BLOB = 250,
  MYSQL_TYPE_LONG_BLOB = 251,
  MYSQL_TYPE_BLOB = 252,
  MYSQL_TYPE_VAR_STRING = 253,
  MYSQL_TYPE_STRING = 254,
  MYSQL_TYPE_GEOMETRY = 255
} enum_field_types;

#endif //THIRD_CONTEST_COMMON_H
