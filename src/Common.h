//
// Created by alexfxzhang on 2021/6/12.
//

#ifndef THIRD_CONTEST_COMMON_H
#define THIRD_CONTEST_COMMON_H

#include <string>
#include <unistd.h>

const std::string DATABASE_NAME = "tianchi_dts_data";                                                             // 待处理数据库名，无需修改
const std::string SCHEMA_FILE_DIR = "schema_info_dir";                                                            // schema文件夹，无需修改。
const std::string SCHEMA_FILE_NAME = "schema.info";                                                               // schema文件名，无需修改。
const std::string SOURCE_FILE_DIR = "source_file_dir";                                                            // 输入文件夹，无需修改。
const std::string SINK_FILE_DIR = "sink_file_dir";                                                                // 输出文件夹，无需修改。
const std::string SOURCE_FILE_NAME_TEMPLATE = "tianchi_dts_source_data_";                                         // 输入文件名，无需修改。
const std::string SINK_FILE_NAME_TEMPLATE = "tianchi_dts_sink_data_";                                             // 输出文件名模板，无需修改。
const std::string CHECK_TABLE_SETS = "customer,district,item,new_orders,order_line,orders,stock,warehouse";       // 待处理表集合，无需修改。
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

typedef enum enum_field_types {
  MYSQL_TYPE_DECIMAL, MYSQL_TYPE_TINY,
  MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG,
  MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE,
  MYSQL_TYPE_NULL, MYSQL_TYPE_TIMESTAMP,
  MYSQL_TYPE_LONGLONG, MYSQL_TYPE_INT24,
  MYSQL_TYPE_DATE, MYSQL_TYPE_TIME,
  MYSQL_TYPE_DATETIME, MYSQL_TYPE_YEAR,
  MYSQL_TYPE_NEWDATE, MYSQL_TYPE_VARCHAR,
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

#include <atomic>

class DstFile {
public:
  std::string path;
  int fd;
  std::atomic_long off;

  DstFile() : fd(0), off(0) {
  }

  ~DstFile() {
    if (fd != 0 && fd != -1) {
      close(fd);
    }
  }


  long write(const std::string &s) {
    long offset = off.fetch_add(s.length());
    pwrite(fd, s.c_str(), s.length(), offset);
    return offset;
  }

  long write(char *s, size_t len) {
    long offset = off.fetch_add(len);
    pwrite(fd, s, len, offset);
    return offset;
  }

  long write(char *s, size_t len, off_t offset) {
    pwrite(fd, s, len, offset);
    return offset;
  }
};

class TmpFile {
public:
  std::vector<DstFile *> files;
  std::vector<DstFile *> indexFiles;

  TmpFile() {
  }

  ~TmpFile() {
    for (auto &file : files) {
      std::string path = file->path;
      delete file;
      remove(path.c_str());
    }
    files.clear();
    for (auto &file : indexFiles) {
      std::string path = file->path;
      delete file;
      remove(path.c_str());
    }
    indexFiles.clear();
  }
};

#endif //THIRD_CONTEST_COMMON_H
