#!/usr/bin/env bash
FILES_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && cd .. && cd files && pwd )"

## Create input stream using DataServer

/usr/local/flink-1.7.1/bin/flink run -c com.chen.guo.WordCountStreaming \
/Users/chguo/Downloads/learn-flink/build/libs/learn-flink-1.0-SNAPSHOT.jar \

## The output can be found at Apache-Flink-Dashboard => Task Managers => Stdout
