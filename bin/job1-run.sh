#!/usr/bin/env bash
/usr/local/flink-1.7.1/bin/flink run -c com.chen.guo.WordCount /Users/chguo/Downloads/learn-flink/build/libs/learn-flink-1.0-SNAPSHOT.jar \
--input file:///Users/chguo/Downloads/learn-flink/files/job1-input.txt \
--output file:///Users/chguo/Downloads/learn-flink/files/job1-output
