
#include <string>
#include <mutex>
#include <shared_mutex>
#include <map>

using namespace std;



class BitmapItem {

public:

    /**
     *
     * @param uniq
     */
    bool putIfAbsent(string uniq);

};


class BitmapManager {
private:
    /**
     *
     */
    std::shared_mutex rwLock;

    /**
     *
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
    void registerBitmap(string schema, string table) {

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
