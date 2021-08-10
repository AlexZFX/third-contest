//
// Created by alexfxzhang on 2021/2/19.
//

#ifndef THIRD_CONTEST_LOGGER_H
#define THIRD_CONTEST_LOGGER_H

#include <cstdio>
#include <string>

// 先放在这里了。后续有机会再搞到独立文件里面
static inline std::string getTimeStr(time_t realTime, const std::string &timeFormat = "%Y-%m-%d %H:%M:%S") {
  char timeBuff[64] = {0};
  strftime(timeBuff, 64, timeFormat.c_str(), localtime(&realTime));
  return std::string(timeBuff);
}

#ifndef LogError
#define LogError(fmt, args...) \
  do { \
    fprintf(stderr, fmt, ##args);\
    fprintf(stderr, "\n");\
  } while(0)
#endif

#ifndef LogInfo
#define LogInfo(fmt, args...) \
  do { \
    printf(fmt, ##args);\
    printf("\n");\
    fflush(nullptr);\
  } while(0)
#endif

#ifndef LogFatal
#define LogFatal(fmt, args...) \
  do { \
    fprintf(stderr, fmt, ##args);\
    fflush(nullptr);\
  } while(0)
#endif

#ifndef LogDebug
#define LogDebug(fmt, args...) \
  do { \
    printf(fmt, ##args);\
    printf("\n");\
    fflush(nullptr);\
  } while(0)
#endif

#endif //THIRD_CONTEST_LOGGER_H