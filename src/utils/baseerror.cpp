#include <cstdarg>
#include <cstdio>

#include "baseerror.h"


int CBaseError::setError(const char *fmt, ...) {
  va_list arg;
  va_start(arg, fmt);
  int ret = vsnprintf(m_errbuf, sizeof(m_errbuf) - 1, fmt, arg);
  va_end(arg);
  return ret;
}
