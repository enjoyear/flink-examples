package com.chen.guo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The items are fed back to the stream itself if a condition is not met
 */
public class StreamIterateOperation {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Tuple2<Long, Integer>> data = env.generateSequence(0, 5).map(new MapFunction<Long, Tuple2<Long, Integer>>() {
      @Override
      public Tuple2<Long, Integer> map(Long value) throws Exception {
        return new Tuple2<Long, Integer>(value, 0);
      }
    });

    // prepare stream for iteration, the stream is (0,0), (1,0), (2,0), ..., (5,0)
    // Note, the iteration stream will terminate after 5000 ms of no incoming data,
    // otherwise the stream will run forever
    IterativeStream<Tuple2<Long, Integer>> iteration = data.iterate(5000);

    // plus one stream: (1,1), (2,1), (3,1), ..., (6,1)
    DataStream<Tuple2<Long, Integer>> plusOne = iteration.map(new MapFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {
      @Override
      public Tuple2<Long, Integer> map(Tuple2<Long, Integer> value) throws Exception {
        if (value.f0 == 10) {
          return value;
        }
        return new Tuple2<>(value.f0 + 1, value.f1 + 1);
      }
    });

    DataStream<Tuple2<Long, Integer>> notEqualToken = plusOne.filter(new FilterFunction<Tuple2<Long, Integer>>() {
      @Override
      public boolean filter(Tuple2<Long, Integer> value) throws Exception {
        return value.f0 != 10;
      }
    });

    //Feed data back to next iteration
    iteration.closeWith(notEqualToken);

    //data not feed back to iteration
    DataStream<Tuple2<Long, Integer>> equalToken = plusOne.filter(new FilterFunction<Tuple2<Long, Integer>>() {
      @Override
      public boolean filter(Tuple2<Long, Integer> value) throws Exception {
        return value.f0 == 10;
      }
    });

    equalToken.writeAsText(ParameterTool.fromArgs(args).get("output"));
    env.execute("Iteration Operation");
  }
}
