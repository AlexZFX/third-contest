#include "newmysql.h"
#include <mysqld_error.h>
#include <cstdio>
#include <algorithm>
#include "strings.hpp"

//获取系统启动以来的时间，单位为毫秒
inline uint64_t getMonotonic_msec() {
  struct timespec tm{};
  clock_gettime(CLOCK_MONOTONIC, &tm); //os启动以来的时间，不受到用户修改时间的影响
  return tm.tv_sec * 1000 + tm.tv_nsec / 1000000;
}

int runSqlStatement(CNewMysql &conn, const std::string &sql, uint64_t *cost = NULL) {
  uint64_t begin = 0;
  if (cost) {
    begin = getMonotonic_msec();
  }
  CSelectResult result;
  int ret = conn.query(sql.c_str(), result);
  if (cost) {
    *cost = getMonotonic_msec() - begin;
  }
  if (ret < 0) {
    if (cost) {
      LogError("cost:%lu msec,run sql  error:%s, \"%s\"\n", *cost, conn.getErr(), sql.c_str());
    } else {
      LogError("run sql  error:%s, \"%s\"\n", conn.getErr(), sql.c_str());
    }

    return ret;
  } else {
    if (cost) {
      LogDebug("cost:%lu msec,run sql \"%s\" sucess,ret:%d\n", *cost, sql.c_str(), ret);
    } else {
      LogDebug("run sql \"%s\" sucess,ret:%d\n", sql.c_str(), ret);
    }

    return ret;
  }
}

int CNewMysql::getBranchVersion() {
  if (-1 == m_branch_version) {
    CSelectResult mysql_version;
    char err[1024];
    if (0 == query("select version()", mysql_version) && 0 != mysql_version.num_rows()) {
      int mainVersion = 0;
      int subVersion = 0;
      if (2 != sscanf(mysql_version[0]["version()"], "%d.%d.%*s", &mainVersion, &subVersion)) {
        snprintf(err, sizeof(err), "get mysql version failed,version string:%s", mysql_version[0]["version()"]);
        setError(err);
      } else {
        //mariadb支持10.0.x、10.1.x、10.4.x；percona支持5.7.x、5.6.x
        if (10 == mainVersion && (0 == subVersion || 1 == subVersion || 4 == subVersion)) {
          m_branch_version = MYSQL_MARIADB;
        } else if (5 == mainVersion) {
          if (6 == subVersion) {
            m_branch_version = MYSQL_PERCONA_5_6;
          } else if (7 == subVersion) {
            m_branch_version = MYSQL_PERCONA;
          } else
            m_branch_version = MYSQL_UNKNOWN;
        } else if (8 == mainVersion && 0 == subVersion) {
          m_branch_version = MYSQL_8_0;
        } else {
          m_branch_version = MYSQL_UNKNOWN;
        }
      }
    }
  }
  return m_branch_version;
}

bool CNewMysql::init(const string &host,
                     const string &user,
                     const string &pass,
                     bool reconnect,
                     unsigned int port,
                     unsigned int timeout,
                     const string &db,
                     const string &charset,
                     int conntimeout) {
  m_host = host;
  m_user = user;
  m_pass = pass;
  m_db = db;
  m_port = port;
  m_timeout = timeout;
  m_conntimeout = conntimeout;
  m_reconnect = reconnect;
  m_orgReconnect = reconnect;
  // m_isconn = false;
  m_charset = charset;

//    m_mysql = new MYSQL;
//    mysql_init (m_mysql);
//    if (m_timeout > 0)
//    {
//        mysql_options (m_mysql , MYSQL_OPT_CONNECT_TIMEOUT , (const char *) &m_timeout);
//        mysql_options (m_mysql , MYSQL_OPT_READ_TIMEOUT , (const char *) &m_timeout);
//        mysql_options (m_mysql , MYSQL_OPT_WRITE_TIMEOUT , (const char *) &m_timeout);
//    }

// my_bool sys_reconnect = 0;  //关闭系统的自动重连
// mysql_options(m_mysql,MYSQL_OPT_RECONNECT,&sys_reconnect);

  return connect();

}

//bool
//binlog_reader::initMysqlConfVec(std::vector<CNewMysqlConf> &vec, const std::string &hostlist, const std::string &user,
//                                const std::string &pass, char *errbuf) {
//  std::vector<std::string> hostlistVec = strings::tokenize(hostlist, ",");
//  for (auto iter = hostlistVec.begin(); iter != hostlistVec.end(); ++iter) {
//    // 最后一个 _ 做分隔符，避免域名中有 _ 导致的init失败
//    std::size_t splitOff = iter->find_last_of('_');
//    if (splitOff == std::string::npos) {
//      SecSnprintf(errbuf, 1024, "ipport:%s has no split char '_', hostlist:%s", iter->c_str(), hostlist.c_str());
//      return false;
//    } else {
//      CNewMysqlConf conf;
//      conf.m_host = iter->substr(0, splitOff);
//      conf.m_port = atoi(iter->substr(splitOff + 1).c_str());
//      conf.m_user = user;
//      conf.m_pass = pass;
//      vec.push_back(conf);
//    }
//  }
//  return true;
//}

bool CNewMysql::init(const string &host,
                     unsigned int port,
                     const string &user,
                     const string &pass,
                     unsigned int timeout,
                     const string &db,
                     const string &charset) {
  return init(host, user, pass, true, port, timeout, db, charset);
}

bool CNewMysql::realConnect() {
  MYSQL *tmp = nullptr;

  if (m_host[0] >= '0' && m_host[0] <= '9') { //说明host确实是数字ip
    tmp = mysql_real_connect(m_mysql, m_host.c_str(), m_user.c_str(), m_pass.c_str(), nullptr, m_port,
                             nullptr, 0);
  } else //非数字ip,可能是套接字或者域名
  {
    //检测是否是域名add by seven 20180308
    if (std::string::npos == m_host.find_last_of("/\\")) {
      tmp = mysql_real_connect(m_mysql, m_host.c_str(), m_user.c_str(), m_pass.c_str(), nullptr, m_port,
                               nullptr, 0);
    } else {
      tmp = mysql_real_connect(m_mysql,
                               nullptr, m_user.c_str(), m_pass.c_str(), nullptr, 0, m_host.c_str(), 0);
    }
  }

  if (tmp != nullptr) {
    if (!m_db.empty()) {
      if (0 != mysql_select_db(m_mysql, m_db.c_str())) {
        m_lastErrNo = mysql_errno(m_mysql);
        setError("mysql_select_db db:%s,error:%d,%s", m_db.c_str(), mysql_errno(m_mysql), mysql_error(m_mysql));
        return false;
      }
    }
  } else {
    m_lastErrNo = mysql_errno(m_mysql);
    setError("connect error[%s], errno:%d", mysql_error(m_mysql), mysql_errno(m_mysql));
    return false;
  }

  m_lastOpTime = time(nullptr);

  return queryAutocommit();
}

bool CNewMysql::connect() {
  if (m_mysql) {
    setError("connect error[不需要重复连接]");
    return false;
  }

  if (m_host.empty()) {
    setError("connect error[host is empty]");
    return false;
  }

  m_mysql = new MYSQL;
  mysql_init(m_mysql);

#ifdef MYSQL_OPT_SUPPORT_MILLIS_TIMEOUT
  mysql_options (m_mysql , MYSQL_OPT_MILLIS_TIMEOUT , (const char *) &m_millsSecond);
#endif

  if (!m_conntimeout) {
    m_conntimeout = m_timeout;
  }

  if (m_conntimeout > 0) {
    mysql_options(m_mysql, MYSQL_OPT_CONNECT_TIMEOUT, (const char *) &m_conntimeout);
  }
  //解决蜜罐漏洞，防止连到伪客户端
  if (!m_allowLocalInf) {
    int localInfileFlag = 0;
    if (0 != mysql_options(m_mysql, MYSQL_OPT_LOCAL_INFILE, (const char *) &localInfileFlag)) {
      setError("set option MYSQL_OPT_LOCAL_INFILE failed");
      return false;
    }
  }

  if (m_timeout > 0) {
    //mysql_options (m_mysql , MYSQL_OPT_CONNECT_TIMEOUT , (const char *) &m_timeout);
    mysql_options(m_mysql, MYSQL_OPT_READ_TIMEOUT, (const char *) &m_timeout);
    mysql_options(m_mysql, MYSQL_OPT_WRITE_TIMEOUT, (const char *) &m_timeout);
  }
  if (!m_charset.empty()) {
    mysql_options(m_mysql, MYSQL_SET_CHARSET_NAME, m_charset.c_str());
    //  printf("mysql_options MYSQL_SET_CHARSET_NAME %s\n",m_charset.c_str());
  }
  int localInfile = 1;
  mysql_options(m_mysql, MYSQL_OPT_LOCAL_INFILE, &localInfile);
  if (!realConnect()) {
    close();
    return false;
  } else {
    return true;
  }

  //  return true;
}

bool CNewMysql::queryAutocommit() {
  if (!m_mysql) {
    setError("is not connected");
    return false;
  }

  const char *sql = "set session autocommit=1";
  if (!m_autocommit) {
    sql = "set session autocommit=0";
  }

  if (mysql_query(m_mysql, sql) != 0) {
    m_lastErrNo = mysql_errno(m_mysql);
    setError("query error[%s], errno:%d,sql[%s]", mysql_error(m_mysql), mysql_errno(m_mysql), sql);
    close();
    return false;
  } else {
    m_lastOpTime = time(NULL);

    //unsigned long numRows = 0;
    MYSQL_RES *result = mysql_store_result(m_mysql);
    if (NULL == result) {
      //   printf ("run sql:%s success\n" , sql);
      return true;
    } else {
      setError("mysql_store_result should not  return true");
      mysql_free_result(result);
      close();
      return false;
    }
  }

}

//可以重复执行
void CNewMysql::close() {
  if (m_mysql) {
    mysql_close(m_mysql);
    delete m_mysql;
    m_mysql = NULL;
  }
//    if (m_isconn)
//    {
//        mysql_close (m_mysql);
//        m_isconn = false;
//    }
}

CNewMysql::~CNewMysql() {
  close();
  if (NULL != m_mysql) {
    delete m_mysql;
    m_mysql = NULL;
  }
}

//如果是select语句，返回0成功，否则失败
//如果是update,delete,insert语句，>0表示影响的行数，0表示无影响，<0表示失败
//如果返回值是ConnectFail，表示连接有问题，可以考虑切换到备机等,如使用CMSNewMysql，则可以忽略这一条;
int CNewMysql::query(const char *sql, CSelectResult &selectResult, bool recurse) {

  if (selectResult.getMysqlResult()) { //有没有读取完毕的数据，肯定是异常了，直接清理掉这个session吧
    fprintf(stderr,
            "CNewMysql::query selectResult.getMysqlResult is not null,fatal error,will close the session\n ");
    selectResult.clear();
    close();
  }

  selectResult.clear();
  m_lastErrNo = 0;

  if (!recurse) {
    if (m_reconnect && abs(time(NULL) - m_lastOpTime) > 50) { //超过50秒没有更新
      close(); //关闭连接
    }
  }

  if (!checkConnect()) {
    //setError ("query error[mysql not connected], sql[%s]"  , sql);
    return ConnectFail; //
  }

  unsigned long numRows;

  if (mysql_query(m_mysql, sql) != 0) {
    unsigned int err = mysql_errno(m_mysql);
    m_lastErrNo = mysql_errno(m_mysql);
#define CR_SERVER_GONE_ERROR 2006
#define CR_SERVER_LOST 2013
#define ER_DUP_ENTRY 1062
#define CR_COMMANDS_OUT_OF_SYNC 2014
#define CR_UNKNOWN_ERROR 2000
    if (err == CR_SERVER_GONE_ERROR || err == CR_SERVER_LOST) {
      setError("query error[%s], errno:%d,sql[%s],may retry:%d", mysql_error(m_mysql), mysql_errno(m_mysql), sql,
               recurse);
      close();
      if (recurse && m_reconnect) {
        return query(sql, selectResult, false); //再查询一次
      } else {
        setError("query error[gone away agin], errno:%d,sql[%s]", err, sql);
        return -1;
      }
    } else if (err == ER_DUP_ENTRY) //insert 操作,key重复
    {
      m_lastOpTime = time(nullptr);
      setError("key duplacate");
      return 0;
    } else if (err == CR_COMMANDS_OUT_OF_SYNC || err == CR_UNKNOWN_ERROR) {
      setError("will close,query error[%s], errno:%d,sql[%s]", mysql_error(m_mysql), mysql_errno(m_mysql), sql);
      close();
      return -1;
    } else {
      setError("query error[%s], errno:%d,sql[%s]", mysql_error(m_mysql), mysql_errno(m_mysql), sql);
      return -1;
    }
  } else {
    m_lastOpTime = time(nullptr);

    MYSQL_RES *result = nullptr;
    if (selectResult.isOneByOne()) {
      result = mysql_use_result(m_mysql);
    } else {
      result = mysql_store_result(m_mysql);
    }

    if (nullptr == result) {
      if (mysql_field_count(m_mysql) == 0) { //insert ,update and so on
        numRows = mysql_affected_rows(m_mysql);
        if (numRows != (unsigned long) -1) {
          return numRows;
        } else {
          m_lastErrNo = mysql_errno(m_mysql);

          setError("query error[%s,mysql_affected_rows return -1 ], errno:%d,sql[%s]", mysql_error(m_mysql),
                   mysql_errno(m_mysql), sql);
          return -1;
        }

      } else //error
      {
        m_lastErrNo = mysql_errno(m_mysql);

        setError("query error[%s,mysql_store_result return null,but mysql_field_count !=0 ], errno:%d,sql[%s]",
                 mysql_error(m_mysql),
                 mysql_errno(m_mysql), sql);
        return -1;
      }
    } else {
      selectResult.alalyzeResult(this, result);
    }

    return 0;
  }
}

bool CNewMysql::begin() {
  int ret = runSqlStatement(*this, "begin");
  if (0 != ret) {
    setError("begin error:%d", ret);
    close(); //失败直接断开连接
    return false;
  }
  return true;
}

bool CNewMysql::commit() {
  int ret = runSqlStatement(*this, "commit");
  if (0 != ret) {
    setError("commit error:%d", ret);
    close(); //失败直接断开连接
    return false;
  }
  return true;
}

void CNewMysql::rollback() {
  int ret = runSqlStatement(*this, "rollback");
  if (0 != ret) {
    setError("rollback error:%d", ret);
    close(); //回滚失败直接断开连接
  }
}

bool CNewMysql::checkConnect(bool force) {
  if (m_mysql) {
    return true;
  }
  if ((!m_reconnect) && (!force)) { //如果不做自动重连
    char buff[1024];
    SecSnprintf(buff, 1024, "%s, and not support reconnect", getErr());
    setError("%s", buff);
    return false;
  }
  return connect();
}

bool CNewMysql::useDb(const char *db) {
  m_db = db; //这样赋值后以后每次建立连接都会自动选择db

  if (!checkConnect()) { //没有连接，可以返回成功，下次重连的时候会设置db
    return true;
  }

  if (mysql_select_db(m_mysql, m_db.c_str()) == 0) {
    return true;
  } else {
    setError("useDb[%s] error[%s], errno:%d", db, mysql_error(m_mysql), mysql_errno(m_mysql));

    m_lastErrNo = mysql_errno(m_mysql);

    int err = mysql_errno(m_mysql);
    if (err == CR_SERVER_GONE_ERROR || err == CR_SERVER_LOST) {
      close();
    }

    return false;
  }
}

bool CNewMysql::setCharset(const string &charset) {
  if (charset == m_charset) {
    return true;
  }

  m_charset = charset;
  if (!checkConnect()) //没有连接，可以返回成功，下次重连的时候会设置db
  {
    return true;
  }

  if (mysql_set_character_set(m_mysql, m_charset.c_str()) != 0) {
    setError("mysql_set_charset_name[%s] error[%s], errno:%d", m_charset.c_str(), mysql_error(m_mysql),
             mysql_errno(m_mysql));
    m_lastErrNo = mysql_errno(m_mysql);

    int err = mysql_errno(m_mysql);
    if (err == CR_SERVER_GONE_ERROR || err == CR_SERVER_LOST) {
      close();
    }
    return false;
  } else {
    //printf ("mysql_set_character_set:%s success\n" , m_charset.c_str ());
  }

  return true;
}

CRowResult *CSelectResult::getNextRow() {
  if (!m_mysqlResult) {
    return NULL;
  }

  if (m_lastRow) {
    delete m_lastRow;
    m_lastRow = NULL;
  }

  MYSQL_ROW row = mysql_fetch_row(m_mysqlResult);
  if (!row) { //todo 已经没有新记录了
    freeMysqlResult();
    return NULL;
  }
  unsigned long *lens = mysql_fetch_lengths(m_mysqlResult);
  m_lastRow = new CRowResult(m_str_int_map);
  m_lastRow->init(row, lens, m_num_field);

  return m_lastRow;
}

bool CSelectResult::alalyzeResult(CNewMysql *mysql, MYSQL_RES *res) {
  clear();

  m_mysql = mysql;
  m_thread_id = mysql->getThreadId();
//    printf ("threadid %lu\n" , m_thread_id);

  m_num_field = mysql_num_fields(res);
  MYSQL_FIELD *fields = mysql_fetch_fields(res);
  for (unsigned int i = 0; i < m_num_field; ++i) {
    m_str_int_map[fields[i].name] = i;
    m_int_str_map[i] = fields[i].name;
  }

  if (!m_oneByOne) {
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res))) {
      unsigned long *lens = mysql_fetch_lengths(res);
      auto *rowResult = new CRowResult(m_str_int_map);
      rowResult->init(row, lens, m_num_field);
      m_vec.push_back(rowResult);
    }

    mysql_free_result(res);
  } else {
    m_mysqlResult = res;
  }

  return true;
}

void CSelectResult::clear() {

  freeMysqlResult();

  m_num_field = 0;
  m_str_int_map.clear();
  m_int_str_map.clear();

  for (auto iter = m_vec.begin(); iter != m_vec.end(); ++iter) {
    delete (*iter);
  }
  m_vec.clear();
}

void CSelectResult::freeMysqlResult() {

  if (m_lastRow) {
    delete m_lastRow;
    m_lastRow = nullptr;
  }

  if (m_mysqlResult) {
    //检测句柄是否还有效
    if (m_thread_id != 0 && m_mysql && m_thread_id == m_mysql->getThreadId()) {
      //            printf ("handle is valid,will end\n");
      ;//句柄还有效，需要释放
    } else {
      //            printf ("handle is not valid\n");
      m_mysqlResult->handle = NULL; //句柄无效了
    }
    mysql_free_result(m_mysqlResult);
    m_mysqlResult = NULL;
  }
}

void CRowResult::init(MYSQL_ROW row, unsigned long *lens, unsigned int num_field) {
  m_num = num_field;
  if (num_field == 0) {
    return;
  }

  m_result = new charptr[num_field * 2];
  m_lens = m_result + num_field;
  for (unsigned int i = 0; i < num_field; ++i) {
    int len = lens[i];
    //   if ((NULL == row[i]) || (len == 0))
    if ((NULL == row[i])) { //如长度为0，不建议设置为NULL
      m_result[i] = (char *) "NULL";
      m_lens[i] = (char *) 4;
    } else {
      m_result[i] = new char[len + 1];
      memcpy(m_result[i], row[i], len);
      m_result[i][len] = 0;
      m_lens[i] = (char *) len;
    }
  }
}

CRowResult::~CRowResult() {
  if (m_result) {
    for (unsigned int i = 0; i < m_num; ++i) {
      if (m_result[i] != (char *) "NULL") {
        delete[] m_result[i];
      }
    }

    delete[] m_result;
  }

}
