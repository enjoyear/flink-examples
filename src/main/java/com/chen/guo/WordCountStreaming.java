package com.chen.guo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStreaming {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);
    DataStream<String> text = env.socketTextStream("localhost", 9999);
    DataStream<String> filtered = text.filter(new FilterFunction<String>() {
      @Override
      public boolean filter(String value) throws Exception {
        return value.length() > 1;
      }
    });

    DataStream<Tuple2<String, Integer>> tokenized = filtered.map(new MapFunction<String, Tuple2<String, Integer>>() {
      @Override
      public Tuple2<String, Integer> map(String value) throws Exception {
        return new Tuple2<>(value, 1);
      }
    });
    //It will create separate substreams for different keys in the tuple
    DataStream<Tuple2<String, Integer>> counts = tokenized.keyBy(0).sum(1);

    counts.print();

    env.execute("Streaming Word Count");
  }
}
