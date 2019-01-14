package com.chen.guo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

public class AverageProfitGlobalWindow {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    /**
     * Flow:
     * event produced-> network -> message queue -> received by Flink(flink ingestion) -> processed by Flink
     *
     *
     * ProcessingTime: system time of the machine which executes task
     *                 the cheapest method of forming windows and the method that introduces the least latency
     *                 it requires no co-ordination between streams and machines
     * IngestionTime: the time when the event enters into Flink
     * EventTime: the time when the event is produced
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);
    DataStream<String> text = env.socketTextStream("localhost", 9090);

    //month, product, category, profit, count
    DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = text.map(new Splitter());

    //group by month
    DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped
        .keyBy(0)
        .window(GlobalWindows.create())
        .trigger(CountTrigger.of(5))
        .reduce(new Reduce1());

    reduced.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
    env.execute("Avg Profit Per Month");
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
