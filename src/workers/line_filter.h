//
// Created by springliao on 2021/8/11.
//

#ifndef THIRD_CONTEST_LINE_FILTER_H
#define THIRD_CONTEST_LINE_FILTER_H


#include "map"

#include "../common/FileChunk.h"
#include "../utils/BaseThread.h"
#include "loaddata_worker.h"
#include "../entity/Table.h"


class LineFilter {
public:

    /**
     * receive line record
     * @param record
     */
    void onReceive(LineRecord *record);

    LoadDataWorker *getWorker() const {
        return _worker;
    }


    Table *getTable() const {
        return table;
    }

private:
    Table *table;
    LoadDataWorker *_worker;
    ThreadSafeQueue<std::string> *queuePtr;
};


class LineFilterManager : public BaseThread {
public:

    /**
     *
     */
    void dispatch();

protected:
    int run() override;

private:
    bool _isClosed;

    /**
     * key -> schema.table, value -> LineFilter*
     */
    map<string, LineFilter*> _filters;
};

#endif //THIRD_CONTEST_LINE_FILTER_H
