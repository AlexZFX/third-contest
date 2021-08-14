//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_METADATA_H
#define HIGHDTS_METADATA_H

#include <string>
#include <utility>

using namespace std;

// TODO 这里是不是可以考虑用 protobuf 来进行数据的序列化操作
/**
 * @brief Metadata 内部应该有一个任意的数据
 * 
 */
class Metadata {
public:
  Metadata(void *item) {
    _item = item;
  }

  ~Metadata() = default;

  /**
   * @brief 返回一个构造对应元数据的生成器
   *
   * @return void*
   */
  void *supplier();

  /**
   * @brief 加载元数据信息
   *
   * @return bool
   */
  virtual bool deserial();

private:
  void *_item;
};

#endif //HIGHDTS_METADATA_H
