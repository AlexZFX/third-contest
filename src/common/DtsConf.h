//
// Created by alexfxzhang on 2021/6/14.
//

#ifndef THIRD_CONTEST_DTSCONF_H
#define THIRD_CONTEST_DTSCONF_H

#include <string>
#include <unordered_map>

using namespace std;

class DtsConf {
public:
  string inputDir;
  string outputDir;
  string outputDbUrl;
  string outputDbUser;
  string outputDbPass;

  bool readerFinish;
  bool loadFinish;
  bool dispatchLineFinish;
  bool loadFileWriteFinish;

  DtsConf() : readerFinish(false), loadFinish(false), dispatchLineFinish(false), loadFileWriteFinish(false) {

  }

};

#endif //THIRD_CONTEST_DTSCONF_H
