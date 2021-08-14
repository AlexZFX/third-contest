
#include <iostream>
#include <string>
#include <mutex>
#include <shared_mutex>
#include <map>


#include "../entity/Index.h"

using namespace std;


class BitmapItem {

public:
    BitmapItem(long max, Index index[], int indexNum) {
        _bits = new char[max];
        _index = index;
        _indexNum = indexNum;
    }

    /**
     *
     * @param uniq
     */
    bool putIfAbsent(const long *ids) {
        int total = sizeof(*ids);

        if (total != _indexNum) {
            cout << &"index size must be : " << _indexNum << ", but actually : " << total << endl;
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
    map<string, BitmapItem *> _itemMap;

public:
    BitmapManager() = default;

    ~BitmapManager() = default;

    /**
     *
     *
     * @param schema
     * @param table
     */
    void registerBitmap(const string& schema, const string& table, Index *index) {
        int total = sizeof(*index);

        string key = schema + "." + table;

        long max = 0;

        for (int i = 0; i < total; i++) {
            max += index[i].getMax();
        }

        BitmapItem tmpItem(max, index, total);

        BitmapItem *item = &tmpItem;

        _itemMap[key] = item;
    }

    /**
     *  not exist, do put and return true, or return false
     *
     * @param schema
     * @param table
     * @param uniq
     * @return
     */
    bool putIfAbsent(const string& schema, const string& table, long *uniq) {
        lock.lock();
        if (_itemMap.find(schema + "." + table) == _itemMap.end()) {
            return false;
        }
        lock.unlock();

        BitmapItem *item = _itemMap[schema + "." + table];
        return item->putIfAbsent(uniq);
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

