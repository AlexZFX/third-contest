
#include <string>
#include <mutex>
#include <shared_mutex>
#include <map>


#include "../entity/Index.h"

using namespace std;



class BitmapItem {

public:
    BitmapItem(long max, Index index) {
        _bits = new int [max];
        _index = index;
    }

    /**
     *
     * @param uniq
     */
    bool putIfAbsent(int ids[]) {
        _lock.lock();

        _lock.unlock();
    }

private:
    char *_bits;

    Index _index;

    std::mutex _lock;
};


class BitmapManager {
private:
    /**
     * 读写锁
     */
    std::shared_mutex rwLock;

    /**
     * bitmap 统一存储对象
     */
    map<string, BitmapItem*> _itemMap;

public:
    BitmapManager() {

    }

    ~BitmapManager() {

    }

    /**
     *
     *
     * @param schema
     * @param table
     */
    void registerBitmap(string schema, string table, Index index) {

    }

    /**
     *  not exist, do put and return true, or return false
     *
     * @param schema
     * @param table
     * @param uniq
     * @return
     */
    bool putIfAbsent(string schema, string table, string uniq) {
        rwLock.lock();
        if (_itemMap.find(schema + "." + table) == _itemMap.end()) {
            _itemMap[schema + "." + table] = &BitmapItem();
        }
        rwLock.unlock();

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
