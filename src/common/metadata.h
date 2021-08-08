//
// Created by lesss on 2021/8/7.
//

#ifndef HIGHDTS_METADATA_H
#define HIGHDTS_METADATA_H

#include <string>
#include <utility>

using namespace std;

class Metadata {
public:
    Metadata(string savePath) {
        _savePath = std::move(savePath);
    }

    ~Metadata() = default;

    /**
     * 持久化操作
     * @return
     */
    virtual int persistent();

    /**
     * 加载元数据信息
     * @return
     */
    virtual int load();

    /**
     * 获取元数据的保存位置
     * @return
     */
    string getSavePath() {
        return _savePath;
    }

private:
    string _savePath;
};


#endif //HIGHDTS_METADATA_H
