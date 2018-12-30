package com.chen.guo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

public class StreamSplitOperation {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);
    DataStream<String> text = env.readTextFile(params.get("input"));

    SplitStream<Integer> evenOddStream = text.map(new MapFunction<String, Integer>() {
      @Override
      public Integer map(String value) throws Exception {
        return Integer.parseInt(value);
      }
    }).split(new OutputSelector<Integer>() {
      @Override
      public Iterable<String> select(Integer value) {
        if (value % 2 == 0) {
          return Collections.singleton("even");
        } else {
          return Collections.singleton("odd");
        }
      }
    });

    DataStream<Integer> evenStream = evenOddStream.select("even");
    DataStream<Integer> oddStream = evenOddStream.select("odd");
    evenStream.writeAsText(params.get("even-output"));
    oddStream.writeAsText(params.get("odd-output"));
    env.execute("Even Odd Splits");
  }
}
