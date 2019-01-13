#!/usr/bin/env bash
FILES_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && cd .. && cd files && pwd )"

## Create input stream by running DataServer class

/usr/local/flink-1.7.1/bin/flink run -c com.chen.guo.SumOverTumblingWindow \
/Users/chguo/Downloads/learn-flink/build/libs/learn-flink-1.0-SNAPSHOT.jar \
--output file://${FILES_DIR}/tumbling-window-sum.txt
