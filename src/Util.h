//
// Created by alexfxzhang on 2021/6/14.
//

#ifndef THIRD_CONTEST_UTIL_H
#define THIRD_CONTEST_UTIL_H


#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <dirent.h>
#include <vector>
#include <string>

using namespace std;

// 获取目录下类型为 REG 的文件名
bool getFileNames(const std::string &fileDir, vector<string> &files) {
  DIR *dir;
  struct dirent *ptr;
  if ((dir = opendir(fileDir.c_str())) == nullptr) {
    perror("Open dir error...");
    return false;
  }
  while ((ptr = readdir(dir)) != nullptr) {
    if (ptr->d_type == DT_REG) {
      files.emplace_back(ptr->d_name);
    } else {
      continue;
    }
  }
  closedir(dir);
  sort(files.begin(), files.end());
  return true;
}

#endif //THIRD_CONTEST_UTIL_H
