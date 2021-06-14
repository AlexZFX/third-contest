//
// Created by alexfxzhang on 2021/6/14.
//

#ifndef THIRD_CONTEST_COLUMN_H
#define THIRD_CONTEST_COLUMN_H

#include <string>
#include <unordered_map>
#include <unordered_set>

using namespace std;

class Column {
public:

private:
  string column_name;
  int ordinal;
  bool is_unsigned;
  string charset;
  enum_field_types dataType;
  string column_def;
  int length;
  int precision;
  int scale;
public:
  Column(const string &column_name, int ordinal, bool is_unsigned, const string &charset, const string &column_def,
         int length, int precision, int scale) {
    this->column_name = column_name;
    this->ordinal = ordinal;
    this->is_unsigned = is_unsigned;
    this->charset = charset;
    this->column_def = column_def;
    this->length = length;
    this->precision = precision;
    this->scale = scale;
  }

  Column(const string &column_name, int ordinal, enum_field_types type, int length, int precision, int scale) {
    this->column_name = column_name;
    this->ordinal = ordinal;
    this->dataType = type;
    this->length = length;
    this->precision = precision;
    this->scale = scale;
  }

  Column(const unordered_map<string, string> &map) {
    this->column_name = map.at("Name");
    this->ordinal = atoi(map.at("Ordinal").c_str());
    this->is_unsigned = (map.at("Unsigned")[0] == 't') ? true : false;
    this->charset = map.at("CharSet");
    this->column_def = map.at("ColumnDef");
    this->length = atoi(map.at("Length").c_str());
    this->precision = atoi(map.at("Precision").c_str());
    this->scale = atoi(map.at("Scale").c_str());
  }

  bool operator==(const Column &other) const {
    return this->column_name == other.column_name;
  }

  bool operator<(const Column &other) const {
    return this->column_name < other.column_name;
  }

  size_t operator()(const Column &column) const {
    return hash<string>()(column.column_name);
  }

  const string &getColumnName() const {
    return column_name;
  }

  void setColumnName(const string &columnName) {
    column_name = columnName;
  }

  int getOrdinal() const {
    return ordinal;
  }

  void setOrdinal(int ordinal) {
    Column::ordinal = ordinal;
  }

  bool isUnsigned() const {
    return is_unsigned;
  }

  void setIsUnsigned(bool isUnsigned) {
    is_unsigned = isUnsigned;
  }

  const string &getCharset() const {
    return charset;
  }

  void setCharset(const string &charset) {
    Column::charset = charset;
  }

  const string &getColumnDef() const {
    return column_def;
  }

  void setColumnDef(const string &columnDef) {
    column_def = columnDef;
  }

  int getLength() const {
    return length;
  }

  void setLength(int length) {
    Column::length = length;
  }

  int getPrecision() const {
    return precision;
  }

  void setPrecision(int precision) {
    Column::precision = precision;
  }

  int getScale() const {
    return scale;
  }

  void setScale(int scale) {
    Column::scale = scale;
  }
};


#endif //THIRD_CONTEST_COLUMN_H
