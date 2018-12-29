#!/usr/bin/env bash
FILES_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && cd .. && cd files && pwd )"

/usr/local/flink-1.7.1/bin/flink run -c com.chen.guo.WordCount \
/Users/chguo/Downloads/learn-flink/build/libs/learn-flink-1.0-SNAPSHOT.jar \
--input file://${FILES_DIR}/job1-input.txt \
--output file://${FILES_DIR}/job1-output
