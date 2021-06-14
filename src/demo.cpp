#include <iostream>
#include <fstream>
#include <cstdlib>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <getopt.h>
#include "Common.h"
#include "Util.h"
#include "Table.h"
#include "DtsConf.h"

using namespace std;

/**
 * @author dts，just for demo.
 */
class Demo {
public:
  string sourceDirectory;
  string sinkDirectory;

  vector<string> m_loadFiles;
  unordered_map<string, Table> m_tables;

public:
  void initialSchemaInfo(string path, string tables) {
    cout << "Read schema_info_dir/schema.info file and construct table in memory." << endl;

    return;
  }

  void loadSourceData(const string &path) {
    cout << "Read source_file_dir/tianchi_dts_source_data_* file" << endl;
    // 获取对应路径下所有文件名
    getFileNames(path, m_loadFiles);
    return;
  }

  void cleanData() {
    cout << "Clean and sort the source data." << endl;
    return;
  }

  void sinkData(const string path) {
    cout << "Sink the data." << endl;
    return;
  }
};

DtsConf g_conf;

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

  cout << "[Start]\tload schema information." << endl;
  // load schema information.
  cout << "[End]\tload schema information." << endl;

  // load input Start file.
  cout << "[Start]\tload input Start file." << endl;
  cout << "[End]\tload input Start file." << endl;

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
