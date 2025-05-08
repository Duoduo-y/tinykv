#!/bin/bash

# 定义日志文件路径
LOG_FILE="/root/tinykv/kv/test_raftstore/test_raftstore.log"

# 运行测试并将输出保存到临时日志文件
go test -run TestBasic2B 
# >| test_raftstore.log 2>&1

# 检查测试结果是否包含 "FAIL"
if grep -q "FAIL" test_raftstore.log; then
    # 如果测试失败，将日志保存到指定路径
    mv test_raftstore.log "$LOG_FILE"
    echo "测试失败，日志已保存到 $LOG_FILE"
else
    # 如果测试成功，删除临时日志文件
    rm test_raftstore.log
    echo "测试成功，未保存日志"
fi