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
    return (index[0] == idx.index[0] && index[1] == idx.index[1] && index[2] == idx.index[2] &&
            index[3] == idx.index[3]);
  }
};

bool IndexComparator(const Index &idx1, const Index &idx2) {
  for (int i = 0; i < 4; ++i) {
    if (idx1.index[i] != idx2.index[i]) {
      return idx1.index[i] < idx2.index[i];
    }
  }
  return true;
}

#endif //THIRD_CONTEST_INDEX_H
