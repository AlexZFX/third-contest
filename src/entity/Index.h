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
  Index() {}

  int index[4] = {-1, -1, -1, -1};
  int offset;
  int len;
//  int version;

  bool operator==(const Index &idx) {
    return memcmp(index, idx.index, sizeof(int) * 4) == 0;
  }
};

#endif //THIRD_CONTEST_INDEX_H
