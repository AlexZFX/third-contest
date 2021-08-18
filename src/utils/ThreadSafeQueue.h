//
// Created by alexfxzhang on 2021/8/10.
//

#ifndef THIRD_CONTEST_THREADSAFEQUEUE_H
#define THIRD_CONTEST_THREADSAFEQUEUE_H

#include <queue>
#include <stack>
#include <mutex>
#include<condition_variable>

using namespace std;

template<class T, class Container=std::queue<T>>
class ThreadSafeQueue {
public:

  ThreadSafeQueue() = default;

  // enqueue - supports move, copies only if needed. e.g. q.enqueue(move(obj));
  void enqueue(T t) {
    std::lock_guard<std::mutex> lock(m);
    q.push(std::move(t));
    c.notify_one();
  }

  // simple dequeue
  T dequeue() {
    std::unique_lock<std::mutex> lock(m);
    while (empty()) c.wait(lock);
    T rVal = std::move(next(q));
    q.pop();
    return rVal;
  }

  // dequeue with timeout in seconds
  bool dequeue(double timeout_sec, T &rVal) {
    std::unique_lock<std::mutex> lock(m);
    bool isTimeout = false;

    // wait for timeout or value available
    auto maxTime = std::chrono::milliseconds(int(timeout_sec * 1000));
    if (c.wait_for(lock, maxTime, [&]() { return !this->empty(); })) {
      rVal = std::move(next(q));
      q.pop();
      return true;
    } else {
      return false;
    }
  }

  size_t size() const { return q.size(); }

  bool empty() const { return q.empty(); }

  void clear() {
    std::lock_guard<std::mutex> lock(m);
    q = Container();
  }

private:
  Container q;
  mutable std::mutex m;
  std::condition_variable c;

  static T &next(std::stack<T> &s) { return s.top(); }

  static T &next(std::queue<T> &q) { return q.front(); }
};

#endif //THIRD_CONTEST_THREADSAFEQUEUE_H
