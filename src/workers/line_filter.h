//
// Created by springliao on 2021/8/11.
//

#ifndef THIRD_CONTEST_LINE_FILTER_H
#define THIRD_CONTEST_LINE_FILTER_H


#include "map"

#include "../common/FileChunk.h"


class LineFilter {
public:

    /**
     * receive line record
     * @param record
     */
    void onReceive(LineRecord *record);
};


class LineFilterManager {
public:

    /**
     *
     */
    void dispatch();

private:
    bool _isClosed;

    /**
     * key -> schema.table, value -> LineFilter*
     */
    map<string, LineFilter*> _filters;
};

#endif //THIRD_CONTEST_LINE_FILTER_H
