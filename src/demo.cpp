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
#include "common/Common.h"
#include "utils/Util.h"
#include "entity/Table.h"
#include "common/DtsConf.h"
#include "utils/ThreadPool.hpp"
#include "workers/MetadataManager.h"
#include "utils/logger.h"
#include "workers/FileSplitter.h"
#include "workers/LoadDataWorker.h"
#include "workers/FileReader.h"
#include "workers/LineFilter.h"
#include "utils/ChunkSet.h"
#include "workers/LoadFileWriter.h"

using namespace std;

DtsConf g_conf;
ThreadPool *g_threadPool;
unordered_map<TABLE_ID, Table *, TABLE_ID_HASH> g_tableMap;
BitmapManager *g_bitmapManager;
LoadFileWriterMgn *g_loadFileWriterMgn;
int g_maxChunkId = INT32_MAX;
int g_minChunkId = 0;


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
  cout << "[Start]\tload and clean data." << endl;
  // load 的过程中进行数据清洗
  long startTime = getCurrentLocalTimeStamp();
  MetadataManager manager;
  if (!manager.init(g_conf.outputDir)) {
    LogError("metadata init failed, return -1");
    return -1;
  }
  g_bitmapManager = new BitmapManager(); // 全局bitmap
  initTableMap();
  // 其他的启动全都依赖于 manager信息ok否
  std::vector<string> readFiles;
  getFileNames(g_conf.inputDir, readFiles, SOURCE_FILE_NAME_TEMPLATE);
  // 根据文件名逆序生成排序所有需要处理的文件
  sort(readFiles.begin(), readFiles.end(), [](const string &s1, const string &s2) {
    int ns1 = atoi(s1.substr(s1.find_last_of('_') + 1).c_str());
    int ns2 = atoi(s2.substr(s2.find_last_of('_') + 1).c_str());
    return ns1 > ns2;
  });
  auto chunkQueue = new ThreadSafeQueue<FileChunk *>(); // splitter 的 dstQueue
  auto chunkSet = new ChunkSet(manager.successChunkIndex); // read 完的 queue，bitManager 的前置queue
  auto loadDataFileNameQueue = new ThreadSafeQueue<string>(); // 待 load 文件的queue
  // 后续 splitter 是可以优化掉的，只在第一次启动的时候run，后续重启不需要再run一遍，只要记录下来就行
  FileSplitter splitter(readFiles, chunkQueue);
  splitter.start();
  //
  manager.start();
  // loadData 的线程
  LoadDataWorkerMgn loadDataMgn(1, loadDataFileNameQueue);
  loadDataMgn.start();
  // 读文件读线程
  FileReaderMgn fileReaderMgn(1, chunkQueue, chunkSet);
  fileReaderMgn.start();

  LineFilter lineFilter(chunkSet); // 单线程的filter，过滤record
  lineFilter.start();

  g_loadFileWriterMgn = new LoadFileWriterMgn(loadDataFileNameQueue);
  g_loadFileWriterMgn->start();


  loadDataMgn.join();
  LogError("run finish will exit , cost: %lld, ", getCurrentLocalTimeStamp() - startTime);
  return 0;
}
