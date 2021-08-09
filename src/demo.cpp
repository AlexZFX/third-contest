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
#include "Util.h"
#include "Table.h"
#include "DtsConf.h"
#include "ThreadPool.hpp"


using namespace std;

DtsConf g_conf;
ThreadPool *g_threadPool;
unordered_map<string, Table *> g_tableMap;
unordered_map<string, TmpFile *> g_tmpFileMap;
unordered_map<string, DstFile *> g_dstFileMap;


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
 * 目标文件
 * @param dstFileMap
 */
void initDstFileMap(unordered_map<string, DstFile *> &dstFileMap) {
  for (const auto &table : CHECK_TABLE_LIST) {
    auto file = new DstFile();
    string path = g_conf.outputDir + SLASH_SEPARATOR + SINK_FILE_DIR + SLASH_SEPARATOR + SINK_FILE_NAME_TEMPLATE +
                  table;
    file->path = path;
    file->fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
    dstFileMap[table] = file;
  }
}

/**
 * 中间文件
 * @param tmpFileMap
 */
void initTmpFileMap(unordered_map<string, TmpFile *> &tmpFileMap) {
  for (const auto &tableName : CHECK_TABLE_LIST) {
    auto table = g_tableMap[tableName];
    auto file = new TmpFile();
    // 每个表对应一定数量的 table
    for (int i = 0; i < table->fileCount; ++i) {
      auto dstFile = new DstFile();
      // table + i
      string path = g_conf.outputDir + SLASH_SEPARATOR + SINK_FILE_DIR + SLASH_SEPARATOR + SINK_FILE_NAME_TEMPLATE +
                    table->table_name + to_string(i);
      dstFile->path = path;
      dstFile->fd = open(path.c_str(), O_RDWR | O_CREAT , 0644);
      cerr << "init file:" << path << " success, fd:" << dstFile->fd << endl;
      file->files.emplace_back(dstFile);

      // table + index_i
//      auto indexFile = new DstFile();
//      string indexPath =
//        g_conf.outputDir + SLASH_SEPARATOR + SINK_FILE_DIR + SLASH_SEPARATOR + SINK_FILE_NAME_TEMPLATE +
//        table->table_name + "index_" + to_string(i);
//      indexFile->path = path;
//      indexFile->fd = open(indexPath.c_str(), O_RDWR | O_CREAT);
//      cerr << "init index file:" << indexPath << " success, fd:" << indexFile->fd << endl;
//      file->indexFiles.emplace_back(indexFile);
    }
    tmpFileMap[tableName] = file;
  }
}


void initFileMap() {
  mkdir((g_conf.outputDir + SLASH_SEPARATOR + SINK_FILE_DIR).c_str(), S_IRUSR | S_IWUSR | S_IXUSR | S_IRWXG | S_IRWXO);
  initTableMap(g_tableMap);
  initTmpFileMap(g_tmpFileMap);
  initDstFileMap(g_dstFileMap);
}

void clearFileMap() {
  for (const auto &tableName : CHECK_TABLE_LIST) {
    auto table = g_tableMap[tableName];
    // 每个表对应一定数量的 table
    delete g_tmpFileMap[tableName];
  }
  g_tmpFileMap.clear();
  for (const auto &item : g_dstFileMap) {
    string path = item.second->path;
    long len = item.second->off - 1;
    delete item.second;
//    int err = truncate(path.c_str(), len);
//    cerr << path << " " << err << " " << strerror(errno) << endl;
  }
}

// 处理读取时的一行
// I       tianchi_dts_data        orders  85      1       1       2935
void dealReadRow(const string &row) {
  std::vector<string> s = tokenize(row, '\t');
  // s[0] 是 type，s[1] db， s[2] table
  auto table = g_tableMap[s[2]];
  string res;
  // 生成即将写入文件的数据
  for (int i = 3, j = 0; i < s.size(); ++i, ++j) {
    res.append(table->get_column(j).validValue(s[i]));
    if (i != s.size() - 1) {
      res.append("\t");
    } else {
      res.append("\n");
    }
  }
  // 写入 res 到指定文件中
  // 先要根据主键大小做 hash
  int hashKey = table->hash(stoi(s[table->pksOrd[0] + 3]));
  auto tmpfile = g_tmpFileMap[s[2]];
  // hash 完写入对应文件，这里要考虑并发
  long off = tmpfile->files[hashKey]->write(res);
//  Index idx;
//  auto pkO = table->getPkOrders();
//  for (int k = 0; k < pkO.size(); ++k) {
//    idx.index[k] = stoi(s[pkO[k] + 3]);
//  }
//  idx.offset = off;
//  idx.version = 0;
//  tmpfile->indexFiles[hashKey]->write((char *) &idx, sizeof(Index));
};

/**
 * 读取所有文件，hash 后写入 tmp 文件中
 */
void readAndClean() {
  std::vector<string> readFiles;
  getFileNames(g_conf.inputDir + SLASH_SEPARATOR + SOURCE_FILE_DIR + SLASH_SEPARATOR, readFiles,
               SOURCE_FILE_NAME_TEMPLATE);
  std::vector<std::future<bool> > readResults;
  // 对匹配到的表执行全量
  readResults.reserve(readFiles.size());
  for (auto &readFile : readFiles) {
    // lambda 调用
    readResults.emplace_back(
      g_threadPool->enqueue([=] {
        int fd = open(readFile.c_str(), O_RDONLY);
        size_t len = lseek(fd, 0, SEEK_END);
        cerr << "file:" << readFile << " size:" << len << endl;
        // 读 buf 文件，然后写到 tmp 文件里面去
        char *buffer = (char *) mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        // 按行读取并处理
        size_t off = 0;
        while (off < len) {
          char *start = buffer;
          while (off++ < len && *buffer != '\n') {
            ++buffer;
          }
          // 此时的 *buff 不是 '\n'
          char *end = buffer;
          // data
          string s(start, end - start);
          dealReadRow(s);
          // 跳过这个 \n
          ++buffer;
        }
        munmap(buffer, len);
        return true;
      })
    );
  }
  // 处理 lambda函数结果，等待所有全量任务执行完才继续
  for (auto &&result: readResults) {
    if (result.get()) {
      continue;
    }
  }
};

void sortAndWrite() {
  std::vector<string> readFiles;
  std::vector<std::future<bool> > writeResults;
  for (const auto &item : g_tableMap) {
//  {
    string tableName = item.first;
    vector<int> pkOrds = item.second->getPkOrders();
//    auto item = g_tableMap.find(TABLE_WAREHOUSE);
//    string tableName = item->first;
//    vector<int> pkOrds = item->second->getPkOrders();
    writeResults.emplace_back(
      g_threadPool->enqueue([=] {
        auto tmpFile = g_tmpFileMap[tableName];
        for (const auto &file : tmpFile->files) {
          long len = file->off;
          char *bufferStart = (char *) mmap(nullptr, len, PROT_READ, MAP_PRIVATE, file->fd, 0);
          auto buffer = bufferStart;
          // 按行读取并处理
          size_t off = 0;
          vector<Index> idxVec;
          while (off < len) {
            char *start = buffer;
            // 表示每个字段的位置和 idx
            Index idx;
            idx.offset = off;
            while (off++ < len && *buffer != '\n') {
              ++buffer;
            }
            // 此时的 *buff 不是 '\n'
            char *end = buffer;
            // data
            string s(start, end - start);
            vector<string> res = tokenize(s, '\t');

            for (int i = 0; i < pkOrds.size(); ++i) {
              idx.index[i] = stoi(res[pkOrds[i]]);
            }
//            idx.version = 0;
            idx.len = s.length() + 1;
            idxVec.emplace_back(idx);
            // data
            // 跳过这个 \n
            ++buffer;
          }
          sort(idxVec.begin(), idxVec.end(), IndexComparator);
          for (int j = 0; j < idxVec.size(); ++j) {
            if (j + 1 < idxVec.size() && (idxVec[j + 1] == idxVec[j])) {
              continue;
            } else {
              char *writeOff = bufferStart + idxVec[j].offset;
              g_dstFileMap[tableName]->write(writeOff, idxVec[j].len);
            }
          }
          munmap(buffer, len);
        }
        int err = truncate(g_dstFileMap[tableName]->path.c_str(), g_dstFileMap[tableName]->off - 1);
        cerr << g_dstFileMap[tableName]->path << " " << err << " " << strerror(errno) << endl;
        return true;
      }));
  }
  // 处理 lambda函数结果，等待所有全量任务执行完才继续
  for (auto &&result: writeResults) {
    if (result.get()) {
      continue;
    }
  }
};

int64_t getCurrentLocalTimeStamp() {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp = std::chrono::time_point_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now());
  auto tmp = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch());
  return tmp.count();

  // return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
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
  initFileMap();
  g_threadPool = new ThreadPool(25);
  cout << "[Start]\tload and clean data." << endl;
  // load 的过程中进行数据清洗
  long startTime = getCurrentLocalTimeStamp();
  readAndClean();
  cout << "readCleanTime : " << getCurrentLocalTimeStamp() - startTime << endl;
  startTime = getCurrentLocalTimeStamp();
  cout << "[End]\tload and clean data." << endl;
  // load input Start file.
  cout << "[Start]\tload input Start file." << endl;
  sortAndWrite();
  cout << "sortWriteTime : " << getCurrentLocalTimeStamp() - startTime << endl;
  cout << "[End]\tload input Start file." << endl;
  clearFileMap();
  // data clean.
  cout << "[Start]\tdata clean." << endl;
  /*
   * 非法整数数值。如定义为int的列值出现了非法字符，我们统一将其处理为"0"值；
   * 超长浮点数精度。如定义为decimal(3, 2)的列值中出现了小数点后3位，我们对其进行4舍5入；
   * 非法时间数据。如定义为datetime的列值中出现了非法的日期，我们将其统一成"2020-04-01 00:00:00.0"；
   * 超长字符长度。如定义为varchar(16)的列值出现了17个字符，此时我们按照此列的最大长度对列值截断（注意不考虑"\0"因素)。
   */
  cout << "[End]\tdata clean." << endl;

  // sink to target file
  cout << "[Start]\tsink to target file." << endl;

  cout << "[End]\tsink to target file." << endl;

  return 0;
}
