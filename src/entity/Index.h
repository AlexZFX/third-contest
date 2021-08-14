//
// Created by alexfxzhang on 2021/6/14.
//

#ifndef THIRD_CONTEST_INDEX_H
#define THIRD_CONTEST_INDEX_H

#include <string>
#include <unordered_set>
#include <unordered_map>

using namespace std;

class Index {
public:
    Index(int pos, long min, long max) {
        _pos = pos;
        _min = min;
        _max = max;
    }

private:
    int _pos;
    long _min;
    long _max;
};

#endif //THIRD_CONTEST_INDEX_H
