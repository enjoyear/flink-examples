package com.chen.guo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SumOverTumblingWindow {
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
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);
    DataStream<String> text = env.socketTextStream("localhost", 9090);

    //Timestamp, rand
    DataStream<Tuple2<Long, Integer>> sum = text.map(new Splitter())
        //Defines which field is for the event time
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, Integer>>() {
          @Override
          public long extractAscendingTimestamp(Tuple2<Long, Integer> element) {
            return element.f0;
          }
        }).windowAll(
            /**
             * For a sliding window of length 3s and sliding 1s each time
             * SlidingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1))
             *
             * For a session window separated by a fixed gap 3s
             * ProcessingTimeSessionWindows.withGap(Time.seconds(3))
             */
            TumblingEventTimeWindows.of(Time.seconds(3)))


        .reduce(new Reduce1());

    sum.print();
    sum.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
    env.execute("Sum over tumbling window");
  }

  public static class Splitter implements MapFunction<String, Tuple2<Long, Integer>> {
    @Override
    public Tuple2<Long, Integer> map(String value) throws Exception {
      String[] split = value.split(",");
      return new Tuple2<>(Long.parseLong(split[0]), Integer.parseInt(split[1]));
    }
  }

  public static class Reduce1 implements ReduceFunction<Tuple2<Long, Integer>> {
    //The input and output must be the same type
    //TODO: Change to FoldFunction(Deprecated, use AggregateFunction) if you want different
    //input and output types for aggregation
    @Override
    public Tuple2<Long, Integer> reduce(
        Tuple2<Long, Integer> current,
        Tuple2<Long, Integer> prevResult) throws Exception {
      return new Tuple2<>(System.currentTimeMillis(), current.f1 + prevResult.f1);
    }
  }
}
