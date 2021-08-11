#ifndef THIRD_CONTEST_BASEERROR_H
#define THIRD_CONTEST_BASEERROR_H

#include <cstring>

class CBaseError {
public:
  CBaseError() {
    bzero(m_errbuf, sizeof(m_errbuf));
  }

  const char *getErr() const { return m_errbuf; }

  int setError(const char *fmt, ...) __attribute__((format(printf, 2, 3)));

  void clearError() {
    m_errbuf[0] = 0;
  }

protected:
  char m_errbuf[4096]{};
};

#endif

