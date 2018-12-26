package com.chen.guo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

public class WordCount {
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);
    DataSource<String> text = env.readTextFile(params.get("input"));
    FilterOperator<String> filtered = text.filter(new FilterFunction<String>() {
      @Override
      public boolean filter(String value) throws Exception {
        return value.length() > 1;
      }
    });

    DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new MapFunction<String, Tuple2<String, Integer>>() {
      @Override
      public Tuple2<String, Integer> map(String value) throws Exception {
        return new Tuple2<String, Integer>(value, 1);
      }
    });
    DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);

    if (params.has("output")) {
      counts.writeAsCsv(params.get("output"), "\n", " ", FileSystem.WriteMode.OVERWRITE);
      env.execute("Word Count");
    }
  }
}
