//
// Created by alexfxzhang on 2021/6/14.
//

#ifndef THIRD_CONTEST_INDEX_H
#define THIRD_CONTEST_INDEX_H

#include <string>
#include <unordered_set>
#include <unordered_map>

using namespace std;

class Index {
private:
  string index_name;
  unordered_set<string> index_column;
  bool is_pk;
  bool is_uk;
public:
  Index(const unordered_map<string, string> &map) {
    this->index_name = map.at("IndexName");
    this->index_column.insert(map.at("IndexCols"));
    this->is_pk = map.at("Primary")[0] == 't' ? true : false;
    this->is_uk = map.at("Unique")[0] == 't' ? true : false;
  }

  const string &getIndexName() const {
    return index_name;
  }

  void setIndexName(const string &indexName) {
    index_name = indexName;
  }

  const unordered_set<string> &getIndexColumn() const {
    return index_column;
  }

  void setIndexColumn(const unordered_set<string> &indexColumn) {
    index_column = indexColumn;
  }

  bool isPk() const {
    return is_pk;
  }

  void setIsPk(bool isPk) {
    is_pk = isPk;
  }

  bool isUk() const {
    return is_uk;
  }

  void setIsUk(bool isUk) {
    is_uk = isUk;
  }

  bool operator==(const Index &other) {
    return this->index_column == other.index_column;
  }

  bool operator<(const Index &other) {
    return this->index_column.size() < other.index_column.size();
  }
};

#endif //THIRD_CONTEST_INDEX_H
