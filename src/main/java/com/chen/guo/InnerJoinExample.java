package com.chen.guo;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class InnerJoinExample {
  public static void main(String[] args) {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);
    DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(params.get("person"))
        .map(new MapFunction<String, Tuple2<Integer, String>>() {
          @Override
          public Tuple2<Integer, String> map(String value) throws Exception {
            String[] words = value.split(",");
            return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
          }
        });

    DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("location"))
        .map(new MapFunction<String, Tuple2<Integer, String>>() {
          @Override
          public Tuple2<Integer, String> map(String value) throws Exception {
            String[] words = value.split(",");
            return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
          }
        });

    DataSet<Tuple3<Integer, String, String>> joined = personSet.join(locationSet).where(0).equalTo(0)
        .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
          @Override
          public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
            return new Tuple3<>(first.f0, first.f1, second.f1);
          }
        });

    joined.writeAsCsv(params.get("output"), "\n", " ");
    try {
      env.execute("Inner join example");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
