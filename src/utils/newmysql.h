#ifndef THIRD_CONTEST_NEWMYSQL_H
#define THIRD_CONTEST_NEWMYSQL_H

#include <string>
#include <map>
#include <vector>
#include <cstdlib>
#include <vector>
#include <cstdint>
#include "../utils/baseerror.h"
#include "mysql.h"
#include "../utils/Util.h"
#include "logger.h"


using namespace std;

class CRowResult {
public:
  CRowResult(const map<string, int> &str_int_map) :
    m_result(NULL), m_lens(NULL), m_map(str_int_map) {
  }

  void init(MYSQL_ROW row, unsigned long *lens, unsigned int num_field);

  typedef char *charptr;

  char *operator[](const char *name) {
    map<string, int>::const_iterator iter = m_map.find(name);
    if (iter == m_map.end()) {
      return (char *) "NULL";
    }

    int field = iter->second;
    return m_result[field];
  }

  char *operator[](size_t index) {
    if (index >= m_map.size()) {
      return (char *) "NULL";
    }

    return m_result[index];
  }

  int colLen(const char *name) {
    map<string, int>::const_iterator iter = m_map.find(name);
    if (iter == m_map.end()) {
      return 4; //NULL
    }

    int field = iter->second;
    return colLen(field);
  }

  int colLen(size_t index) {
    if (!m_lens) {
      return 0;
    }
    if (index >= m_map.size()) {
      return 4; //NULL
    }
    return (int) (reinterpret_cast<ptrdiff_t> (m_lens[index]));
  }

  ~CRowResult();

  static inline bool CheckRowIsNull(const char *ptr) {
    return (ptr == NULL) || (0 == strcmp(ptr, "NULL"));
  }

private:
  CRowResult(const CRowResult &rht);

  CRowResult &operator=(const CRowResult &rht);

  char **m_result;
  char **m_lens;
  const map<string, int> &m_map;
  unsigned int m_num;

};

class CNewMysql;

class CSelectResult {
public:
  CSelectResult(bool oneByone = false) :
    m_oneByOne(oneByone), m_mysql(NULL), m_mysqlResult(NULL), m_thread_id(0), m_lastRow(NULL), m_num_field(0) {

  }

  bool alalyzeResult(CNewMysql *mysql, MYSQL_RES *res);

  void clear();

  void freeMysqlResult();

  ~CSelectResult() {
    clear();
  }

  CRowResult &operator[](unsigned int i) {
    return *(m_vec[i]); //不做越界检测，应用程序自己负责
  }

  //返回下一行，如果调用getNextRow的时候会将上次的结果析构掉，业务不需要自己析构，也不能缓存起来
  CRowResult *getNextRow();

  unsigned int num_rows() const {
    return m_vec.size();
  }

  map<int, string> &getIntStrMap() {
    return m_int_str_map;
  }

  map<string, int> &getStrIntMap() {
    return m_str_int_map;
  }

  bool isOneByOne() const {
    return m_oneByOne;
  }

  void setOneByOne(bool oneByOne) { //todo 需要测试
    if (!oneByOne && m_oneByOne) { //关闭one by one标志，先要清理掉没有读完的数据
      clear();
    }
    m_oneByOne = oneByOne;
  }

  MYSQL_RES *getMysqlResult() {
    return m_mysqlResult;
  }

  unsigned int getNumField() const {
    return m_num_field;
  }

private:
  bool m_oneByOne; //是否一行行读取
  CNewMysql *m_mysql;
  MYSQL_RES *m_mysqlResult;
  unsigned long m_thread_id; /* Id for connection in server */
  CRowResult *m_lastRow;

  unsigned int m_num_field;
  map<string, int> m_str_int_map;
  map<int, string> m_int_str_map;

  //unsigned int m_rows;
  vector<CRowResult *> m_vec;

};

//mysql的连接配置
struct CNewMysqlConf {

public:
  CNewMysqlConf() :
    m_port(0) {

  }

  friend bool operator==(const CNewMysqlConf &lft, const CNewMysqlConf &rht) {
    return lft.m_host == rht.m_host && lft.m_port == rht.m_port && lft.m_user == rht.m_user &&
           lft.m_pass == rht.m_pass;
//        return true;
  }

  string m_host; //ip或者域套接字地址
  unsigned int m_port; //端口
  string m_user; //用户名
  string m_pass; //密码
};

//hostlist: ip_port,ip_port,...
bool initMysqlConfVec(std::vector<CNewMysqlConf> &vec, const std::string &hostlist, const std::string &user,
                      const std::string &pass, char *errbuf);

#define MYSQL_UNKNOWN 0
#define MYSQL_MARIADB 1
#define MYSQL_PERCONA 2
#define MYSQL_PERCONA_5_6 3
#define MYSQL_8_0 4

class CNewMysql : public CBaseError {
public:
  static const int ConnectFail = -10000; //连接失败，这样的情况需要访问备机

public:
  CNewMysql(bool autocommit = true, bool allowLocalInf = false) :
    m_autocommit(autocommit), m_mysql(nullptr), m_port(0), m_timeout(0), m_conntimeout(0), m_reconnect(true),
    m_orgReconnect(true),
    m_lastOpTime(0),
    m_lastErrNo(0),
    m_branch_version(-1),
    m_allowLocalInf(allowLocalInf) {
  }

  //尽量使用这个
  bool init(const string &host,
            unsigned int port,
            const string &user,
            const string &pass,
            unsigned int timeout = 3,
            const string &db = "",
            const string &charset = "");

  bool init(const string &host,
            const string &user,
            const string &pass,
            bool reconnect = true,
            unsigned int port = 0,
            unsigned int timeout = 3,
            const string &db = "",
            const string &charset = "",
            int conntimeout = 0);

  bool useDb(const char *db);

  bool setCharset(const string &charset);

  int getBranchVersion();

  ~CNewMysql();

//如果是select语句，返回0成功，否则失败
//如果是update,delete,insert语句，>0表示影响的行数，0表示无影响，<0表示失败
//如果返回值是ConnectFail，表示连接有问题，可以考虑切换到备机等,如使用CMSNewMysql，则可以忽略这一条;
  int query(const char *sql, CSelectResult &selectResult, bool recurse = true);

  bool begin();

  bool commit();

  void rollback();

  bool checkConnect(bool force = false);

  inline bool isConnected() const {
    //   return m_isconn;
    return m_mysql != NULL;
  }

  //bool alalyzeResult(MYSQL_RES * res,CSelectResult & selectResult);
  void close();

  int getLastErrno() const {
    return m_lastErrNo;
  }

  const string &getHost() const {
    return m_host;
  }

  const string &getPass() const {
    return m_pass;
  }

  unsigned int getPort() const {
    return m_port;
  }

  unsigned int getTimeout() const {
    return m_timeout;
  }

  const string &getUser() const {
    return m_user;
  }

  MYSQL *getMysql() {
    return m_mysql;
  }

  unsigned long getThreadId() {
    if (m_mysql) {
      return m_mysql->thread_id;
    } else {
      return 0;
    }
  }

  //修改重连标志
  void setReconnect(bool reconnect) {
    m_reconnect = reconnect;
  }

  //恢复重连标志
  void restoreReconnect() {
    m_reconnect = m_orgReconnect;
  }

  const string &getDb() const {
    return m_db;
  }

private:
  bool connect();

  bool realConnect();

  bool queryAutocommit();

  bool m_autocommit;

  MYSQL *m_mysql;

  //string m_dbname;
  string m_host;
  string m_user;
  string m_pass;

  string m_db; //库名

  unsigned int m_port;
  unsigned int m_timeout;
  unsigned int m_conntimeout;

  bool m_reconnect;
  bool m_orgReconnect; //最初的重连标志
  //bool m_isconn;

  string m_charset;

  CNewMysql(const CNewMysql &rht);

  CNewMysql &operator=(const CNewMysql &rht);

  time_t m_lastOpTime; //上次更新时间
  unsigned int m_lastErrNo;

  int m_branch_version;//分支版本
  bool m_allowLocalInf;//是否允许开启MYSQL_OPT_LOCAL_INFILE参数
};

//封装一条CNewMysqlConn连接
struct CNewMysqlConn {
public:
  CNewMysqlConn(const CNewMysqlConf &conf, unsigned int timeout, const string &db, bool reconnect = true,
                int conntimeout = 0) :
    m_conf(conf), m_timeout(timeout), m_db(db), m_reconnect(reconnect), m_conntimeout(conntimeout),
    m_lockedBeginTime(0) {
    m_sqlObj = new CNewMysql;

  }

  bool init() {
    return m_sqlObj->init(m_conf.m_host, m_conf.m_user, m_conf.m_pass, m_reconnect, m_conf.m_port, m_timeout, m_db,
                          "",
                          m_conntimeout);
  }

  inline bool isConnected() {
    return m_sqlObj->isConnected();
  }

  ~CNewMysqlConn() {
    delete m_sqlObj;
  }

  inline CNewMysql *getNewMysql(CNewMysqlConf *conf = NULL) {
    if (conf) {
      *conf = m_conf;
    }

    return m_sqlObj;
  }

  inline time_t getLockedBeginTime() {
    return m_lockedBeginTime;
  }

  inline void setLockedBeginTime(time_t lockedBeginTime) {
    m_lockedBeginTime = lockedBeginTime;
  }

  inline CNewMysqlConf &getConf() {
    return m_conf;
  }

private:

  CNewMysql *m_sqlObj;

  CNewMysqlConf m_conf;

  unsigned int m_timeout;
  string m_db;

  bool m_reconnect;
  int m_conntimeout;

  time_t m_lockedBeginTime; //如果失败则锁定，此处保存锁定的时间

  CNewMysqlConn(const CNewMysqlConn &rht);

  CNewMysqlConn &operator=(const CNewMysqlConn &rht);
};

#endif // THIRD_CONTEST_NEWMYSQL_H

