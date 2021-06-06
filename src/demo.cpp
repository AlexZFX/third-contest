#include <iostream>
#include <string>
#include <fstream>
#include <stdlib.h>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <getopt.h>

using namespace std;

/**
 * @author dts，just for demo.
 */
const string DATABASE_NAME = "tianchi_dts_data";                                                             // 待处理数据库名，无需修改
const string SCHEMA_FILE_DIR = "schema_info_dir";                                                            // schema文件夹，无需修改。
const string SCHEMA_FILE_NAME = "schema.info";                                                               // schema文件名，无需修改。
const string SOURCE_FILE_DIR = "source_file_dir";                                                            // 输入文件夹，无需修改。
const string SINK_FILE_DIR = "sink_file_dir";                                                                // 输出文件夹，无需修改。
const string SOURCE_FILE_NAME_TEMPLATE = "tianchi_dts_source_data_";                                         // 输入文件名，无需修改。
const string SINK_FILE_NAME_TEMPLATE = "tianchi_dts_sink_data_";                                             // 输出文件名模板，无需修改。
const string CHECK_TABLE_SETS = "customer,district,item,new_orders,order_line,orders,stock,warehouse";       // 待处理表集合，无需修改。

class Demo {
    public:
    string sourceDirectory;
    string sinkDirectory;

    public:
    void initialSchemaInfo(string path, string tables) {
        cout << "Read schema_info_dir/schema.info file and construct table in memory." << endl;
        return;
    }

    void loadSourceData(string path) {
        cout << "Read source_file_dir/tianchi_dts_source_data_* file" << endl;
        return;
    }
    
    void cleanData() {
        cout << "Clean and sort the source data." << endl;
        return;
    }

    void sinkData(string path) {
        cout << "Sink the data." << endl;
        return;
    }
};

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
    Demo *demo = new Demo();

    static struct option long_options[] = {
                    {"input_dir",           required_argument, 0,  'i' },
                    {"output_dir",          required_argument, 0,  'o' },
                    {"output_db_url",       required_argument, 0,  'r' },
                    {"output_db_user",      required_argument, 0,  'u' },
                    {"output_db_passwd",    required_argument, 0,  'p' },
                    {0,                     0                , 0,   0 }
               };
    int opt_index;
	int opt;

    while (-1 != (opt = getopt_long(argc, argv, "", long_options, &opt_index))) {

        switch (opt) {
            case 'i' :
                dmo.sourceDirectory = optarg;
                break;
            case 'o' :
                dmo.sourceDirectory = optarg;
                break;
        }
    }

    cout << "[Start]\tload schema information." << endl;
    // load schema information.
    demo->initialSchemaInfo(SCHEMA_FILE_DIR, CHECK_TABLE_SETS);
    cout << "[End]\tload schema information." << endl;

    // load input Start file.
    cout << "[Start]\tload input Start file." << endl;
    demo->loadSourceData(SOURCE_FILE_DIR);
    cout << "[End]\tload input Start file." << endl;

    // data clean.
    cout << "[Start]\tdata clean." << endl;
    demo->cleanData();
    cout << "[End]\tdata clean." << endl;

    // sink to target file
    cout << "[Start]\tsink to target file." << endl;
    demo->sinkData(SINK_FILE_DIR);
    cout << "[End]\tsink to target file." << endl;

    return 0;
}
