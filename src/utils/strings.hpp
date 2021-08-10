#ifndef BINLOG_READER_STRINGS_HPP
#define BINLOG_READER_STRINGS_HPP

#include <algorithm>
#include <string>
#include <map>
#include <vector>

namespace binlog_reader {

  namespace strings {

// Flags indicating how remove should operate.
    enum Mode {
      PREFIX, SUFFIX, ANY
    };

//只删除前缀，只删除后缀，或者任意地方出现的
    inline std::string remove(const std::string &from, const std::string &substring, Mode mode = ANY) {
      std::string result = from;

      if (mode == PREFIX) {
        if (from.find(substring) == 0) {
          result = from.substr(substring.size());
        }
      } else if (mode == SUFFIX) {
        if (from.rfind(substring) == from.size() - substring.size()) {
          result = from.substr(0, from.size() - substring.size());
        }
      } else {
        size_t index;
        while ((index = result.find(substring)) != std::string::npos) {
          result = result.erase(index, substring.size());
        }
      }

      return result;
    }

//删除首尾的chars
    inline std::string trim(const std::string &from, const std::string &chars = " \t\n\r") {
      size_t start = from.find_first_not_of(chars);
      size_t end = from.find_last_not_of(chars);
      if (start == std::string::npos) { // Contains only characters in chars.
        return {};
      }

      return from.substr(start, end + 1 - start);
    }

// Replaces all the occurrences of the 'from' string with the 'to' string.
    inline std::string replace(const std::string &s, const std::string &from, const std::string &to, size_t num = 0) {
      std::string result = s;
      size_t index = 0;

      if (from.empty()) {
        return result;
      }

      size_t count = 0;
      while ((index = result.find(from, index)) != std::string::npos) {
        result.replace(index, from.length(), to);
        index += to.length();
        if (num != 0) {
          ++count;
          if (count >= num) {
            break;
          }
        }
      }
      return result;
    }

// Tokenizes the string using the delimiters.
// Empty tokens will not be included in the result. 优先用这个
    inline std::vector<std::string> tokenize(const std::string &s, const std::string &delims, size_t num = 0) {
      size_t offset = 0;
      std::vector<std::string> tokens;

      while (true) {
        size_t i = s.find_first_not_of(delims, offset);
        if (std::string::npos == i) {
          break;
        }

        size_t j = s.find_first_of(delims, i);
        if (std::string::npos == j) { //已经到了末尾
          tokens.push_back(s.substr(i));
          offset = s.length();
          break;
        }

        if (tokens.size() + 1 == num) { //已经满足了要求，剩余的全部拷贝
          tokens.push_back(s.substr(i));
          break;
        } else {
          tokens.push_back(s.substr(i, j - i));
          offset = j;
        }

      }
      return tokens;
    }

// Splits the string using the provided delimiters.
// Empty tokens are allowed in the result.
    inline std::vector<std::string> split(const std::string &s, const std::string &delims) {
      std::vector<std::string> tokens;
      size_t offset = 0;
      size_t next = 0;

      while (true) {
        next = s.find_first_of(delims, offset);
        if (next == std::string::npos) {
          tokens.push_back(s.substr(offset));
          break;
        }

        tokens.push_back(s.substr(offset, next - offset));
        offset = next + 1;
      }
      return tokens;
    }

    inline bool checkBracketsMatching(const std::string &s, const char openBracket, const char closeBracket) {
      int count = 0;
      for (size_t i = 0; i < s.length(); i++) {
        if (s[i] == openBracket) {
          count++;
        } else if (s[i] == closeBracket) {
          count--;
        }
        if (count < 0) {
          return false;
        }
      }
      return count == 0;
    }

    inline bool startsWith(const std::string &s, const std::string &prefix) {
      return s.find(prefix) == 0;
    }

    inline bool endsWith(const std::string &s, const std::string &suffix) {
      return s.rfind(suffix) == s.length() - suffix.length();
    }

    inline bool contains(const std::string &s, const std::string &substr) {
      return s.find(substr) != std::string::npos;
    }

    inline std::string lower(const std::string &s) {
      std::string result = s;
      std::transform(result.begin(), result.end(), result.begin(), ::tolower);
      return result;
    }

    inline std::string upper(const std::string &s) {
      std::string result = s;
      std::transform(result.begin(), result.end(), result.begin(), ::toupper);
      return result;
    }

  } // namespaces strings {

}

#endif // __STOUT_STRINGS_HPP__
