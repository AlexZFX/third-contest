//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_FILE_UTIL_H
#define HIGHDTS_FILE_UTIL_H

using namespace std;

enum State {
    LOAD_DB_TB_NAME,
    LOAD_COLUMN_NUM,
    LOAD_COLUMNS,
    LOAD_INDEX_NUM,
    LOAD_INDEXES,
    LOAD_PK_NUM,
    LOAD_PKS
};

static State state = State::LOAD_DB_TB_NAME;

static int column_num;
static int index_num;
static int pk_num;

static int pk_ordinal;
static Schema schema;

class FileUtil {
private:

public:
    static void setSchema(const Schema &s) {
        schema = s;
    }

private:
    static void handle_db_table(unordered_map <string, string> ret, Table &table) {
        //        cout << ret.begin()->first << "." << ret.begin()->second << endl;
        table.setDatabaseName(ret.begin()->first);
        table.setTableName(ret.begin()->second);
    }

    static void handle_columns(unordered_map <string, string> ret, Table &table) {
        Column column(ret);
        table.addColumn(column);
    }

    static void hand_indexes(unordered_map <string, string> ret, Table &table) {
        //        not use
        //        Index index(ret);
        //        table.addIndex(index);
    }

/**
 * add pk and its ordinal
 * @param ret
 * @param table
 */
    static void handle_pks(unordered_map <string, string> ret, Table &table) {
        string pk(ret["IndexCols"]);
        table.add_pk_name(pk, pk_ordinal++);
    }

public:
    static State get_state() {
        return state;
    }

/**
 *
 * @param line
 * @param table
 * @return true: finish one table, false: in process
 */
    static bool deserialize_schema(string &line, Table &table) {
        State old_state = state;
        unordered_map <string, string> ret = _deserialize_schema_with_state(line);
        switch (old_state) {
            case LOAD_DB_TB_NAME:
                pk_ordinal = 0;
                handle_db_table(ret, table);
                break;
            case LOAD_COLUMN_NUM:
                break;
            case LOAD_COLUMNS:
                handle_columns(ret, table);
                break;
            case LOAD_INDEX_NUM:
                break;
            case LOAD_INDEXES:
                hand_indexes(ret, table);
                break;
            case LOAD_PK_NUM:
                break;
            case LOAD_PKS:
                handle_pks(ret, table);
                break;
            default:
                break;
        }
        if (state == LOAD_DB_TB_NAME) {
            return true;
        } else {
            return false;
        }
    }

/**
 *
 * @param line
 * @return
 */
    static unordered_map <string, string> _deserialize_schema_with_state(string &line) {
        switch (state) {
            case LOAD_DB_TB_NAME:
                return load_db_table(line);
            case LOAD_COLUMN_NUM:
                return load_tag_num(line, "COLUMN_NUM");
            case LOAD_COLUMNS:
                return load_json(line, LOAD_COLUMNS);
            case LOAD_INDEX_NUM:
                return load_tag_num(line, "INDEX_NUM");
            case LOAD_INDEXES:
                return load_json(line, LOAD_INDEXES);
            case LOAD_PK_NUM:
                return load_tag_num(line, "PRIMARY_KEY_NUM");
            case LOAD_PKS:
                return load_json(line, LOAD_PKS);
            default:
                break;
        }
        return unordered_map<string, string>();
    }

//[DATABASE] tianchi_dts_data [TABLE] item
//first: db_name, second: tb_name
    static unordered_map <string, string> load_db_table(const string &line) {
        column_num = 0;
        index_num = 0;
        pk_num = 0;
        unordered_map <string, string> db_tb_name;
        int db_start = -1;
        int db_end = -1;
        int tb_start = -1;
        for (int i = 0; i < line.size(); i++) {
            if (line[i] == ' ') {
                if (db_start == -1) {
                    db_start = i + 1;
                } else if (db_end == -1) {
                    db_end = i - 1;
                } else if (tb_start == -1) {
                    tb_start = i + 1;
                }
            }
        }
        string db_name = line.substr(db_start, db_end - db_start + 1);
        string tb_name = line.substr(tb_start);
        db_tb_name[db_name] = tb_name;
        state = LOAD_COLUMN_NUM;
        return db_tb_name;
    }

// 3 situations:
// COLUMN NUMBER 5
// first(fixed): COLUMN_NUM, second: num

// INDEX NUMBER 1
// first(fixed): INDEX_NUMBER, second: num

// PRIMARY KEY NUMBER 1
// first(fixed): PRIMARY_KEY_NUMBER, second: num
    static unordered_map <string, string> load_tag_num(const string &line, const string tag) {
        unordered_map <string, string> tag_name_num;
        int num_start = 0;
        for (int i = line.size() - 1; i >= 0; i--) {
            if (line[i] == ' ') {
                num_start = i + 1;
                break;
            }
        }
        tag_name_num[tag] = line.substr(num_start);
        //COLUMN_NUM / INDEX_NUM / PRIMARY_KEY_NUM
        if (tag[0] == 'C') {
            column_num = atoi(line.substr(num_start).c_str());
            state = LOAD_COLUMNS;
        } else if (tag[0] == 'I') {
            index_num = atoi(line.substr(num_start).c_str());
            state = LOAD_INDEXES;
        } else if (tag[0] == 'P') {
            pk_num = atoi(line.substr(num_start).c_str());
            state = LOAD_PKS;
        }
        return tag_name_num;
    }

/**
 * load json
 * @param line
 * @param cur_state
 * @return
 */
// {"Name":"i_id","Ordinal":1,"Unsigned":false,"CharSet":null,"ColumnDef":"int(11)","Length":null,"Precision":10,"Scale":0}
// first: name, second: value
    static unordered_map <string, string> load_json(string &line, State cur_state) {
        unordered_map <string, string> ret;
        if (line.empty()) {
            return ret;
        }
        switch (cur_state) {
            case LOAD_COLUMNS:
                if (--column_num == 0) {
                    state = LOAD_INDEX_NUM;
                }
                break;
            case LOAD_INDEXES:
                if (--index_num == 0) {
                    state = LOAD_PK_NUM;
                }
                break;
            case LOAD_PKS:
                if (--pk_num == 0) {
                    state = LOAD_DB_TB_NAME;
                }
                break;
            default:
                break;
        }
        // add comma for convenience
        line += ",\"";
        int comma_position = -1;
        for (int i = 0; i < line.size(); i++) {
            if (line[i] == ',' && (line[i - 1] == '\"' || line[i + 1] == '\"')) {
                string pair_string = line.substr(comma_position + 1, i - 1 - (comma_position + 1) + 1);
                for (int j = 0; j < pair_string.size(); j++) {
                    int colon_position = -1;
                    if (pair_string[j] == ':') {
                        colon_position = j;
                        string dirty_first = pair_string.substr(0, colon_position);
                        string dirty_second = pair_string.substr(colon_position + 1);

                        string first = clean_string(dirty_first);
                        string second = clean_string(dirty_second);
                        ret.insert({first, second});
                    }
                }
                comma_position = i;
            }
        }
        return ret;
    }

// I	tianchi_dts_data	orders	5	1	1	1517	2021-04-11 15:41:55.0	9	6	1
    static void load_source_data(unordered_map <string, set<Record>> &records, const string &line) {
        vector <string> split_line;
        split_line.reserve(line.size());
        int pre_tab_position = -1;
        int size = line.size();
        for (int i = 0; i < size; i++) {
            if (line[i] == '\t') {
                //                cout << line.substr(pre_tab_position + 1, (i - 1 - pre_tab_position)) << ", ";
                split_line.push_back(line.substr(pre_tab_position + 1, (i - 1 - pre_tab_position)));
                pre_tab_position = i;
            }
        }
        split_line.push_back(line.substr(pre_tab_position + 1, (size - 1 - pre_tab_position)));
        //        cout << line.substr(pre_tab_position + 1, (size - 1 - pre_tab_position)) << endl;
        handle_split_line(records, split_line);
    }

private:
    static string clean_string_with_char(const string &dirty_string, const char left, const char right) {
        //start == -1 means not meet first wanted character
        int start = -1;
        int end = dirty_string.size() - 1;
        for (int i = 0; i < dirty_string.size(); i++) {
            if (start == -1 && (dirty_string[i] == '\"' || dirty_string[i] == '{' || dirty_string[i] == '['
                                || dirty_string[i] == ' ' || dirty_string[i] == left)) {
                continue;
            } else if (start != -1 && (dirty_string[i] == '\"' || dirty_string[i] == '}' || dirty_string[i] == ']'
                                       || dirty_string[i] == ' ' || dirty_string[i] == right)) {
                end = i - 1;
                break;
            }
            if (start == -1) {
                start = i;
            }
        }
        string ret = dirty_string.substr(start, end - start + 1);
        return ret;
    }

    static string clean_string(const string &dirty_string) {
        return clean_string_with_char(dirty_string, ' ', ' ');
    }

    static void handle_split_line(unordered_map <string, set<Record>> &records, const vector <string> &split_line) {
        if (split_line.size() <= 3) {
            cout << "Error line." << endl;
        }
        string db_name = split_line[1];
        string tb_name = split_line[2];
        Table table = schema.get_table(tb_name);
        unordered_map<string, int> pks = table.get_pk_names();
        vector <string> pk_values;
        pk_values.resize(pks.size());
        vector <Column> columns = table.get_columns();
        Record record(db_name, tb_name);
        record.setTable(table);
        for (int i = 0; i < columns.size(); i++) {
            string col_name = columns[i].getColumnName();
            string value = split_line[3 + i];
            //get pks info to sort the record
            if (pks.find(col_name) != pks.end()) {
                int index = pks[col_name];
                pk_values[index] = value;
            }
            //todo: optimize clean data
            clean_data(columns[i], value);
            record.add_column(col_name, value);
        }
        record.set_pk_values(pk_values);
        // first insert
        if (records.find(tb_name) == records.end()) {
            set <Record> record_set;
            record_set.insert(record);
            records.insert({tb_name, record_set});
        } else {
            records.at(tb_name).insert(record);
        }
    }

//1) exceed char length data;
//2) error date time type data;
//3) error decimal type data;
//4) error data type.
    static void clean_data(Column column, string &value) {
        string column_def = column.getColumnDef();
        //1) exceed char length data;
        string char_tag = "char";
        int char_index = column_def.find(char_tag);
        if (char_index != string::npos) {
            string char_num_s = clean_string_with_char(column_def.substr(char_index + char_tag.size()), '(', ')');
            int char_num = atoi(char_num_s.c_str());
            if (value.size() > char_num) {
                value = value.substr(0, char_num);
            }
            return;
        }
        //2) error date time type data;
        string datetime_tag = "datetime";
        string correct_datetime = "2020-04-01 00:00:00.0";
        if (strcmp(column_def.c_str(), datetime_tag.c_str()) == 0) {
            for (int i = 0; i < value.size(); i++) {
                if (value[i] == '-' || value[i] == ' ' || value[i] == ':' || value[i] == '.' ||
                    (value[i] >= '0' && value[i] <= '9')) {
                    continue;
                } else {
                    value = correct_datetime;
                    return;
                }
            }
        }
        //3) error decimal type data;
        string decimal_tag = "decimal";
        int decimal_index = column_def.find(decimal_tag);
        if (decimal_index != string::npos) {
            //(4,2) ===> 4,2
            string decimal_str = clean_string_with_char(column_def.substr(decimal_index + decimal_tag.size()), '(',
                                                        ')');
            int comma = decimal_str.find(',');
            //浮点位数
            int f_num = atoi(decimal_str.substr(comma + 1).c_str());
            int period = value.find(".");
            if (period != string::npos) {
                if (value.size() - period - 1 > f_num) {
                    if (value[period + f_num + 1] < '5') {
                        value = value.substr(0, period + f_num + 1);
                    } else {
                        value = value.substr(0, period + f_num + 1);
                        //string先转成float，再在最后一位+1
                        float f = atof(value.c_str());
                        bool negative = f < 0.0f;

                        //进位
                        float delta = 1.0f;
                        int f_num_ = f_num;
                        while (f_num_--) {
                            delta /= 10.0f;
                        }
                        if (!negative) {
                            f += delta;
                        } else {
                            f -= delta;
                        }
                        ostringstream buffer;
                        buffer << f;
                        value = buffer.str();
                        int count = (period + f_num + 1) - value.size();
                        //补0, (float)3.40 -> (string)3.4 -> (string)3.40(补上)
                        //special case: 34.0 -> 34 -> 34.0
                        if (count > 0) {
                            if (value.find('.') == string::npos) {
                                count--;
                                value += '.';
                            }
                            while (count-- > 0) {
                                value += '0';
                            }
                        }
                    }
                    return;
                }
            }
        }
        //4) error data type.
        string int_tag = "int";
        if (column_def.find(int_tag) != string::npos || column_def.find(decimal_tag) != string::npos) {
            for (int i = 0; i < value.size(); i++) {
                if (value[i] == '-' || value[i] == ' ' || value[i] == ':' || value[i] == '.' ||
                    (value[i] >= '0' && value[i] <= '9')) {
                    continue;
                } else {
                    value = "0";
                    return;
                }
            }
        }
    }
};

#endif //HIGHDTS_FILE_UTIL_H
