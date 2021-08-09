//
// Created by alexfxzhang on 2021/6/14.
//

#ifndef THIRD_CONTEST_TABLE_H
#define THIRD_CONTEST_TABLE_H

#include "Column.h"
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <iostream>

#include "Index.h"

using namespace std;

class Table {
public:
  string database_name;
  string table_name;

  vector<Column> columns;
//    unordered_map<string, Column> columns;
  int maxPkNum;
  int fileCount;
  int hashNum;

  // first: pk_name, second: ordinal
  unordered_map<string, int> pks;
  vector<int> pksOrd;

  // 500 25个文件 则 hashNum = 20
  void setHashNum(int maxPkNum, int fileCount) {
    this->maxPkNum = maxPkNum;
    this->fileCount = fileCount;
    this->hashNum = maxPkNum / fileCount;
  }

public:
  int hash(int keyNum) {
    return (keyNum - 1) / hashNum;
  }

  const string &getDatabaseName() const {
    return database_name;
  }

  void setDatabaseName(const string &databaseName) {
    database_name = databaseName;
  }

  const string &getTableName() const {
    return table_name;
  }

  void setTableName(const string &tableName) {
    table_name = tableName;
  }


  void addColumn(const Column &column) {
    columns.push_back(column);
  }


  void add_pk_name(const string &pk, const int ordinal) {
    pks.insert({pk, ordinal});
  }

  unordered_map<string, int> get_pk_names() {
    return pks;
  }

  const vector<int> &getPkOrders() const {
    return pksOrd;
  }

  void reset() {
    database_name.clear();
    table_name.clear();
    columns.clear();
    pks.clear();
  }

  vector<Column> get_columns() {
    return columns;
  }

  const Column &get_column(int ordinal) {
    if (ordinal < 0 || ordinal > columns.size()) {
      cout << "ordinal error." << endl;
    }
    return columns.at(ordinal);
  }

  void toString() {
    cout << database_name << "." << table_name << ", total: " << columns.size() << " column, columns: [";
    int count = 1;
    for (const auto &column : columns) {
      cout << column.getColumnName() << "(" << column.getOrdinal() << ")";
      if (count++ != columns.size()) {
        cout << ", ";
      }
    }
    cout << "], ";
    for (const auto &pk : pks) {
      cout << pk.first << "(" << pk.second << "), ";
    }
    cout << endl;
  }
};

#endif //THIRD_CONTEST_TABLE_H
