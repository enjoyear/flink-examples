package com.chen.guo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamReduceOperation {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);
    DataStream<String> text = env.readTextFile(params.get("input"));

    //month, product, category, profit, count
    DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = text.map(new Splitter());

    //group by month
    DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped
        .keyBy(0)
        //Rolling reduce
        .reduce(new Reduce1());
    /**
     * Notes:
     * 1. min v.s. minBy or max v.s. maxBy
     *  min and max don't care about other fields other than the aggregated fields,
     *  thus would potential given wrong mappings for the min/max value.
     *
     *  use minBy and maxBy if you want other fields.
     *
     * 2. Provide field name string if the input type is a class.
     */


    //month, avg profit
    DataStream<Tuple2<String, Double>> profitPerMonth = reduced.map(new MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>>() {
      @Override
      public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> value) throws Exception {
        return new Tuple2<>(value.f0, value.f3 * 1.0 / value.f4);
      }
    });

    profitPerMonth.print();

    env.execute("Streaming Word Count");
  }

  public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {

    @Override
    public Tuple5<String, String, String, Integer, Integer> map(String value) throws Exception {
      String[] split = value.split(",");
      return new Tuple5<>(split[0], split[1], split[2], Integer.parseInt(split[4]), 1);
    }
  }

  public static class Reduce1 implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {
    //The input and output must be the same type
    //TODO: Change to FoldFunction(Deprecated, use AggregateFunction) if you want different
    //input and output types for aggregation
    @Override
    public Tuple5<String, String, String, Integer, Integer> reduce(
        Tuple5<String, String, String, Integer, Integer> current,
        Tuple5<String, String, String, Integer, Integer> prevResult) throws Exception {
      return new Tuple5<>(current.f0, current.f1, current.f2,
          current.f3 + prevResult.f3, current.f4 + prevResult.f4);
    }
  }
}
