//
// Created by springliao on 2021/8/11.
//

#include "line_filter.h"
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

void LineFilter::onReceive(LineRecord *record) {

}
