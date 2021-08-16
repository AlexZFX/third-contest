
#include <iostream>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>


#include "common/Common.h"
#include "entity/Index.h"

using namespace std;


class BitmapItem {

  public:
  BitmapItem(long max, Index index[], int indexNum) {
    _max = max;
    _bits = new char[max];
    _index = index;
    _indexNum = indexNum;
  }

  ~BitmapItem() {
    delete[] _bits;
  }

  /**
   * 这个东西是单线程调用的，不加锁也一样
   * @param uniq
   */
  bool putIfAbsent(const int *ids) {
    long preMin = 0;
    long preMax = 0;

    for (int i = 0; i < _indexNum; ++i) {
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

  /**
   * 无锁判断某个记录是否在 bitmap 中
   *
   * @param ids
   * @return if exist return true, otherwise return false
   */
  bool checkExistsNoLock(const int *ids) {
    long preMin = 0;
    long preMax = 0;

    for (int i = 0; i < _indexNum; ++i) {
      Index idx = _index[i];
      const long val = ids[i];
      preMin = preMax;
      preMax += idx.getMax();

      if (_bits[preMin + val] == 1) {
        return true;
      }
      preMax++;
    }
    return false;
  }


  private:
  char *_bits;

  int _indexNum;

  long _max;

  Index *_index;

  std::mutex _lock;

  public:
  char *getBits() const {
    return _bits;
  }
  void setBits(char *bits) {
    _bits = bits;
  }
  int getIndexNum() const {
    return _indexNum;
  }
  Index *getIndex() const {
    return _index;
  }
  long getMax() const {
    return _max;
  }
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

  // bitMapManager 的 snapshot
  stack<char *> _lastSnapshot;

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
    lock.lock();
    long maxCharArr = 0;
    for (auto &item : _itemMap) {
      maxCharArr += item.second->getMax();
    }

    // 这里将所有的 bitmap 数据存放在一个 char* 数组中，单个 bitmap 元素的格式 => TABLE_ID@[bitmap data], 每个 bitmap 之间的数据 => [bitmap item]#[bitmap item]
    char *serialBitArr = new char [maxCharArr + 8 * 4];
    long pos = 0;

    map<TABLE_ID, char *> bitData = map<TABLE_ID, char *>();
    for (auto &item : _itemMap) {
      char *copyInfo = new char[item.second->getMax() + 3];
      memcpy(copyInfo + 2, item.second->getBits(), item.second->getMax());
      copyInfo[0] = item.first;
      copyInfo[1] = '@';
      copyInfo[item.second->getMax() + 2] = '#';
      memcpy(serialBitArr + pos, copyInfo, item.second->getMax() + 3);
      pos += (item.second->getMax() + 3);
    }

    // 将最新的 snapshot 压到栈中
    _lastSnapshot.push(serialBitArr);

    lock.unlock();
  }

  /**
   *
   */
  void loadSnapshot(char * data) {
    long total = sizeof(*data);
    long prePos = 0;

    lock.lock();

    for (long i = 0; i < total; i ++) {
      if (data[i] == '#') {
        char *tmp = new char [i - prePos];
        memcpy(tmp, data + prePos, i - prePos);
        auto id = static_cast<TABLE_ID>(tmp[0]);

        // 加载新的 bitmap 的snapshot 到本地
        _itemMap[id]->setBits(tmp + 2);
      }
    }

    lock.unlock();
  }
};

extern BitmapManager *g_bitmapManager;
