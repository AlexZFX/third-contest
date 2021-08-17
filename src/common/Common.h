//
// Created by alexfxzhang on 2021/6/12.
//

#ifndef THIRD_CONTEST_COMMON_H
#define THIRD_CONTEST_COMMON_H

#include <string>
#include <unistd.h>

/**
 * @brief 按照每个chunk 16M 进行处理
 *
 */
const long PerChunkSize = 16 * 1024 * 1024;
const long LoadFileSize = 2 * 1024 * 1024;

const std::string DATABASE_NAME = "tianchi_dts_data";                                                       // 待处理数据库名，无需修改
const std::string SCHEMA_FILE_DIR = "schema_info_dir";                                                      // schema文件夹，无需修改。
const std::string SCHEMA_FILE_NAME = "schema.info";                                                         // schema文件名，无需修改。
const std::string SOURCE_FILE_DIR = "source_file_dir";                                                      // 输入文件夹，无需修改。
const std::string SINK_FILE_DIR = "sink_file_dir";                                                          // 输出文件夹，无需修改。
const std::string SOURCE_FILE_NAME_TEMPLATE = "tianchi_dts_rematch_data_";                                   // 输入文件名，无需修改。
const std::string SINK_FILE_NAME_TEMPLATE = "tianchi_dts_sink_data_";                                       // 输出文件名模板，无需修改。
//const std::string CHECK_TABLE_SETS = "customer,district,item,new_orders,order_line,orders,stock,warehouse"; // 待处理表集合，无需修改。
//const std::vector<std::string> CHECK_TABLE_LIST = {"customer", "district", "item", "new_orders", "order_line", "orders",
//                                                   "stock", "warehouse"};
const std::string LOAD_FILE_DIR = "load_file_dir";
const std::string META_DIR = "meta_dir";

const std::string TABLE_WAREHOUSE = "warehouse";
const std::string TABLE_DISTRICT = "district";
const std::string TABLE_CUSTOMER = "customer";
const std::string TABLE_NEW_ORDERS = "new_orders";
const std::string TABLE_ORDERS = "orders";
const std::string TABLE_ORDERS_LINE = "order_line";
const std::string TABLE_ITEM = "item";
const std::string TABLE_STOCK = "stock";

enum class TABLE_ID : int {
  TABLE_WAREHOUSE_ID = 1,
  TABLE_DISTRICT_ID = 2,
  TABLE_CUSTOMER_ID = 3,
  TABLE_NEW_ORDERS_ID = 4,
  TABLE_ORDERS_ID = 5,
  TABLE_ORDERS_LINE_ID = 6,
  TABLE_ITEM_ID = 7,
  TABLE_STOCK_ID = 8,
  TABLE_UNKNOWN = -1
};

struct TABLE_ID_HASH {
  template<typename T>
  std::size_t operator()(T t) const {
    return static_cast<std::size_t>(t);
  }
};

inline TABLE_ID getTableIdByName(const std::string &tableName) {
  if (tableName == TABLE_WAREHOUSE) {
    return TABLE_ID::TABLE_WAREHOUSE_ID;
  } else if (tableName == TABLE_CUSTOMER) {
    return TABLE_ID::TABLE_CUSTOMER_ID;
  } else if (tableName == TABLE_DISTRICT) {
    return TABLE_ID::TABLE_DISTRICT_ID;
  } else if (tableName == TABLE_NEW_ORDERS) {
    return TABLE_ID::TABLE_NEW_ORDERS_ID;
  } else if (tableName == TABLE_ORDERS) {
    return TABLE_ID::TABLE_ORDERS_ID;
  } else if (tableName == TABLE_ORDERS_LINE) {
    return TABLE_ID::TABLE_ORDERS_LINE_ID;
  } else if (tableName == TABLE_ITEM) {
    return TABLE_ID::TABLE_ITEM_ID;
  } else if (tableName == TABLE_STOCK) {
    return TABLE_ID::TABLE_STOCK_ID;
  } else {
    return TABLE_ID::TABLE_UNKNOWN;
  }
}


const std::string SLASH_SEPARATOR = "/";

const std::string DATE_REGEX_STR = ".*[a-zA-Z]+.*";
const std::string DECIMAL_2_FORMAT = "%.2lf";
const std::string DECIMAL_0_FORMAT = "%.0lf";

const char INSERT_OPERATION_DESC = 'I';
const char DELETE_OPERATION_DESC = 'D';
const char BEFORE_DATE_IMG_DESC = 'A';
const char AFTER_DATE_IMG_DESC = 'B';

const std::string LoadSuccessFileName = "loadSuccessFiles";
const std::string SuccessChunkIdName = "successChunkId";


typedef enum OPERATION {
  INSERT_OPERATION = 1,
  DELETE_OPERATION = -1,
  BEFORE_DATE_IMG = 4,
  AFTER_DATE_IMG = 5,
  UNKNOWN = -1,
} OPERATION;

//const std::string ITEM_LOAD_SQL = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE " + TABLE_ITEM +
//                                  " FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'"
//                                  " LINES TERMINATED BY '\\n' STARTING BY '' (i_data,i_id,i_im_id,i_name,i_price)";
const std::string ITEM_LOAD_SQL = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE " + TABLE_ITEM +
                                  "(i_data,i_id,i_im_id,i_name,i_price)";

const std::string DISTRICT_LOAD_SQL = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE " + TABLE_DISTRICT +
                                      "(d_city,d_id,d_name,d_next_o_id,d_state,d_street_1,d_street_2,d_tax,d_w_id,d_ytd,d_zip)";

//const std::string ORDER_LINE_LOAD_SQL = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE " + TABLE_ORDERS_LINE +
//                                        " FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'"
//                                        " LINES TERMINATED BY '\\n' STARTING BY '' (ol_amount,ol_d_id,ol_delivery_d,ol_dist_info,ol_i_id,ol_number,ol_o_id,ol_quantity,ol_supply_w_id,ol_w_id)";
const std::string ORDER_LINE_LOAD_SQL = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE " + TABLE_ORDERS_LINE +
                                        "(ol_amount,ol_d_id,ol_delivery_d,ol_dist_info,ol_i_id,ol_number,ol_o_id,ol_quantity,ol_supply_w_id,ol_w_id)";

const std::string ORDERS_LOAD_SQL = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE " + TABLE_ORDERS +
                                    "(o_all_local,o_c_id,o_carrier_id,o_d_id,o_entry_d,o_id,o_ol_cnt,o_w_id)";

const std::string STOCK_LOAD_SQL = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE " + TABLE_STOCK +
                                   "(s_data,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_i_id,s_order_cnt,s_quantity,s_remote_cnt,s_w_id,s_ytd)";

const std::string WAREHOUSE_LOAD_SQL = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE " + TABLE_WAREHOUSE +
                                       "(w_city,w_id,w_name,w_state,w_street_1,w_street_2,w_tax,w_ytd,w_zip)";


//const std::string NEW_ORDERS_LOAD_SQL = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE " + TABLE_NEW_ORDERS +
//                                        " FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'"
//                                        " LINES TERMINATED BY '\\n' STARTING BY '' (no_d_id,no_o_id,no_w_id)";
const std::string NEW_ORDERS_LOAD_SQL = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE " + TABLE_NEW_ORDERS +
                                        "(no_d_id,no_o_id,no_w_id)";

const std::string CUSTOMER_LOAD_SQL = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE " + TABLE_CUSTOMER +
                                      "(c_balance,c_city,c_credit,c_credit_lim,c_d_id,c_data,c_delivery_cnt,c_discount,c_first,c_id,c_last,c_middle,c_payment_cnt,c_phone,c_since,c_state,c_street_1,c_street_2,c_w_id,c_ytd_payment,c_zip)";


/**
 * @brief Get the Op By Desc object
 * 
 * @param opDesc 
 * @return const int 
 */
inline OPERATION getOpByDesc(char c) {
  switch (c) {
    case INSERT_OPERATION_DESC:
      return INSERT_OPERATION;
    case DELETE_OPERATION_DESC:
      return DELETE_OPERATION;
    case BEFORE_DATE_IMG_DESC:
      return BEFORE_DATE_IMG;
    case AFTER_DATE_IMG_DESC:
      return AFTER_DATE_IMG;
    default:
      return UNKNOWN;
  }
}

#endif //THIRD_CONTEST_COMMON_H
