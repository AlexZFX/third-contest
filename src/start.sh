#!/bin/sh
#
# Author: dts
# Date: 2021/05/13
#
APP_HOME=demo.cpp
OUT_NAME=demo
g++ $APP_HOME -std=c++11 -o $OUT_NAME

#(函数)启动程序
echo "start"

./$OUT_NAME $@

echo "end"

# r --input_dir /root/ft_local/test_source --output_dir /root/ft_local/test_dst
