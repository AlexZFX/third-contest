#include <iostream>
#include <fstream>
#include <cstdlib>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <getopt.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "Common.h"
#include "utils/Util.h"
#include "Table.h"
#include "DtsConf.h"
#include "utils/ThreadPool.hpp"


using namespace std;

DtsConf g_conf;
ThreadPool *g_threadPool;
unordered_map<string, Table *> g_tableMap;

/**
 * 初始化参数
 * @param argc
 * @param argv
 */
void initArg(int argc, char *argv[]) {
  int opt;
  int opt_index;
  static struct option long_options[] = {
    {"input_dir",        required_argument, nullptr, 'i'},
    {"output_dir",       required_argument, nullptr, 'o'},
    {"output_db_url",    required_argument, nullptr, 'r'},
    {"output_db_user",   required_argument, nullptr, 'u'},
    {"output_db_passwd", required_argument, nullptr, 'p'},
    {nullptr, 0,                            nullptr, 0}
  };
  while (-1 != (opt = getopt_long(argc, argv, "", long_options, &opt_index))) {
    switch (opt) {
      case 'i' : {
        g_conf.inputDir = optarg;
        break;
      }
      case 'o' : {
        g_conf.outputDir = optarg;
        break;
      }
      case 'r': {
        g_conf.outputDbUrl = optarg;
        break;
      }
      case 'u': {
        g_conf.outputDbUser = optarg;
        break;
      }
      case 'p': {
        g_conf.outputDbPass = optarg;
        break;
      }
      default: {
        cerr << "error conf" << endl;
        break;
      }
    }
  }

}

int64_t getCurrentLocalTimeStamp() {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp = std::chrono::time_point_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now());
  auto tmp = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch());
  return tmp.count();
}

/**
Input: 
1. Disordered source data (in SOURCE_FILE_DIR)
2. Schema information (in SCHEMA_FILE_DIR)

Process:
    data clean: 
    1) duplicate primary key data;
    2) exceed char length data;
    3) error date time type data;
    4) error decimal type data;
    5) error data type.

    sort by pk

Output:
1. Sorted data of each table (out SINK_FILE_DIR)

**/
int main(int argc, char *argv[]) {
  // 初始化 g_conf
  initArg(argc, argv);
  g_threadPool = new ThreadPool(25);
  cout << "[Start]\tload and clean data." << endl;
  // load 的过程中进行数据清洗
  long startTime = getCurrentLocalTimeStamp();
  cout << "readCleanTime : " << getCurrentLocalTimeStamp() - startTime << endl;
  startTime = getCurrentLocalTimeStamp();
  cout << "[End]\tload and clean data." << endl;
  // load input Start file.
  cout << "[Start]\tload input Start file." << endl;
  cout << "sortWriteTime : " << getCurrentLocalTimeStamp() - startTime << endl;
  cout << "[End]\tload input Start file." << endl;
  // data clean.
  cout << "[Start]\tdata clean." << endl;
  cout << "[End]\tdata clean." << endl;
  // sink to target file
  cout << "[Start]\tsink to target file." << endl;

  cout << "[End]\tsink to target file." << endl;

  return 0;
}
