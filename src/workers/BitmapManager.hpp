
#include <iostream>
#include <string>
#include <mutex>
#include <shared_mutex>
#include <map>


#include "../entity/Index.h"
#include "../common/Common.h"

using namespace std;


class BitmapItem {

public:
  BitmapItem(long max, Index index[], int indexNum) {
    _bits = new char[max];
    _index = index;
    _indexNum = indexNum;
  }

  ~BitmapItem() {
    delete[]_bits;
  }

  /**
   * 这个东西是单线程调用的，不加锁也一样
   * @param uniq
   */
  bool putIfAbsent(const int *ids) {
    int total = sizeof(*ids);

    if (total != _indexNum) {
      cout << "index size must be : " << _indexNum << ", but actually : " << total << endl;
      return false;
    }

    long preMin = 0;
    long preMax = 0;

    for (int i = 0; i < total; ++i) {
      Index idx = _index[i];
      const long val = ids[i];
      preMin = preMax;
      preMax += idx.getMax();

      _lock.lock();

      if (_bits[preMin + val] == 1) {
        _lock.unlock();
        return false;
      }
      _bits[preMin + val] = 1;
      _lock.unlock();

      preMax++;
    }
    return true;
  }

  bool checkExistsNoLock(const int *ids) {

  }

private:
  char *_bits;

  int _indexNum;

  Index *_index;

  std::mutex _lock;
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
  map<TABLE_ID, BitmapItem *> _itemMap;

public:
  BitmapManager() = default;

  ~BitmapManager() = default;

  /**
   *
   *
   * @param schema
   * @param table
   */
  void registerBitmap(TABLE_ID tableId, Index *index) {
    int total = sizeof(*index);
    long max = 0;
    for (int i = 0; i < total; i++) {
      max += index[i].getMax();
    }
    BitmapItem tmpItem(max, index, total);
    BitmapItem *item = &tmpItem;
    _itemMap[tableId] = item;
  }

  /**
   *  not exist, do put and return true, or return false
   *
   * @param schema
   * @param table
   * @param uniq
   * @return
   */
  bool putIfAbsent(TABLE_ID tableId, int *uniq) {
    BitmapItem *item = _itemMap[tableId];
    return item->putIfAbsent(uniq);
  }

  /**
   * 判断对应的 line 是否已经存在，不加锁
   * @param tableIds
   * @return
   */
  bool checkExistsNoLock(TABLE_ID tableId, int *idx) {
    return _itemMap[tableId]->checkExistsNoLock(idx);
  }

  /**
   *
   */
  void doSnapshot() {

  }

  /**
   *
   */
  void loadSnapshot() {

  }
};

BitmapManager *BitmapMgn;

