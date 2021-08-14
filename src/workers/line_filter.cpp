//
// Created by springliao on 2021/8/11.
//


#include "../common/Common.h"
#include "line_filter.h"
#include "bitmap_manager.hpp"
#include "global_obj.hpp"

void LineFilterManager::dispatch() {

    for (; ;) {
        if (_isClosed) {
            return;
        }

        LineRecord *record = LineRecordMQ->dequeue();

        const string table = record->schema + "." + record->table;

        LineFilter *filter = _filters[table];

        filter->onReceive(record);
    }

}

int LineFilterManager::run() {
    dispatch();
    return 0;
}

void LineFilter::onReceive(LineRecord *record) {
    //
    if (record->operation == BEFORE_DATE_IMG) {
        return;
    }

    bool exist = BitmapMgn->putIfAbsent(record->schema, record->table, record->idxs);
    if (exist) {
        cout << "data exist, so skip" << endl;
        return;
    }

    if (record->operation == DELETE_OPERATION) {
        return;
    }

    // do data clean
    const Table *table = getTable();
    char *val = new char [8];
    memcpy(val, record->fields + record->datetimeStartPos, 8);

    vector<Column> columns = table->columns;
    for (const Column& column : columns) {
        if (column.getDataType() == MYSQL_TYPE_DATETIME) {
            //TODO
        }
    }


    // enqueue line to queue;
    queuePtr->enqueue(record->fields);
}



