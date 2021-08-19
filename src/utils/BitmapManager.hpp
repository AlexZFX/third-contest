
#ifndef THIRD_CONTEST_BITMAPMANAGER_H
#define THIRD_CONTEST_BITMAPMANAGER_H

#include "boost/thread/mutex.hpp"
#include <iostream>
#include <lib/parallel_hashmap/phmap.h>
#include <lib/parallel_hashmap/phmap_dump.h>
#include <mutex>
#include <string>


#include "common/Common.h"
#include "entity/Index.h"
#include "parallel_hashmap/phmap.h"
#include "logger.h"

using namespace std;

class BitmapItem {

public:
  BitmapItem() {
  }

  ~BitmapItem() {
  }

  /**
   *
   * @param ids
   * @return true 如果不存在，false 如果已存在
   */
  bool putIfAbsent(const std::string &key) {
    if (m_indexSet.contains(key)) {
      return false;
    }
    m_indexSet.insert(key);
    return true;
  }

  void put(const std::string &key) {
    m_indexSet.insert(key);
  }

  /**
   * 无锁判断某个记录是否在 bitmap 中
   *
   * @param ids
   * @return if exist return true, otherwise return false
   */
  bool checkExistsNoLock(const std::string &key) {
    return m_indexSet.contains(key);
  }

private:
  phmap::parallel_flat_hash_set<std::string, phmap::priv::hash_default_hash<string>,
    phmap::priv::hash_default_eq<string>, phmap::priv::Allocator<string>, 8, boost::mutex> m_indexSet;

public:

};


class BitmapManager {
private:
  /**
   * 读写锁
   */
  std::mutex lock;
  /**
   * bitmap 统一存储对象
   */
//  phmap::flat_hash_map<TABLE_ID, BitmapItem *> _itemMap;

  BitmapItem *bitItems[8];

  // bitMapManager 的 snapshot
//  stack<char *> _lastSnapshot;

public:
  BitmapManager() = default;

  ~BitmapManager() = default;

  /**
   *
   *
   * @param schema
   * @param table
   */
  void registerBitmap(TABLE_ID tableId) {
    auto item = new BitmapItem();
    bitItems[static_cast<int>(tableId) - 1] = item;
  }

  /**
   *  not exist, do put and return true, or return false
   *
   * @param schema
   * @param table
   * @param uniq
   * @return
   */
  bool putIfAbsent(TABLE_ID tableId, const string &key) {
    BitmapItem *item = bitItems[static_cast<int>(tableId) - 1];
    return item->putIfAbsent(key);
  }

  /**
   * 判断对应的 line 是否已经存在
   * @param tableIds
   * @return
   */
  bool checkExistsNoLock(TABLE_ID tableId, const string &key) {
    return bitItems[static_cast<int>(tableId) - 1]->checkExistsNoLock(key);
  }

  void put(TABLE_ID tableId, const string &key) {
    bitItems[static_cast<int>(tableId) - 1]->put(key);
  }

};

extern BitmapManager *g_bitmapManager;

#endif