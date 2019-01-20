package com.chen.guo.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Demonstrate the CheckPointing using the ValueState
 */
public class StateDemo {
  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * By default checkpointing is disabled.
     * Enable by creating a checkpoint every 1000 ms.
     *
     * It plays an important role in Flink to decide when to inject a Barrier inside the stream
     */
    env.enableCheckpointing(1000);

    /**
     * After a snapshot(or checkpoint) is taken, it is saved to a state backend(e.g. HDFS).
     * It may take a long time for this checkpointing to complete, which might be even longer than
     * the checkpointing interval
     *
     * It makes sure that minimum progress time to happen between checkpoints.
     * e.g. Given checkpointing every 1000ms and checkpointing take 900ms to complete,
     * then we are left with only 100ms to process the data
     *
     * Another alternative would be using ASYNCHRONOUS checkpointing, but even if in that case,
     * most of our resources are exhausted with checkpointing tasks.
     */
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

    /**
     * checkpoints have to complete within 10000 ms, or are discarded
     */
    env.getCheckpointConfig().setCheckpointTimeout(10000);

    /**
     * Set the guarantee level
     * set mode to exactly-once (this is the default)
     *
     * AT_LEAST_ONCE is used when the latency is a concern
     */
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

    /**
     * allow only one checkpoint to be in progress(may overlap) at the same time
     */
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    /**
     * DELETE_ON_CANCELLATION is the default value
     * State will be deleted if the job is cancelled
     *
     * enable externalized checkpoints which are retained after job cancellation
     */
    env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    /**
     * The restart strategy can also be configured in the Flink Yaml file
     *
     * 1. Fixed Delay Restart Strategy:
     * RestartStrategies.fixedDelayRestart(max no. of restart attempts, delay time between attempts))
     *
     * 2. Failure Rate Restart Strategy: Flink will keep on trying restarting till failure rate exceeds
     *    FailureRate = No. of failures per time interval
     * RestartStrategies.failureRateRestart(failure rate, time interval for measuring failure rate, delay)
     *
     * 3. No Restart Strategy
     * 4. Fallback Restart Strategy: The cluster defined restart strategy is used
     *
     */
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100));
    // number of restart attempts , delay in each restart

    DataStream<String> data = env.socketTextStream("localhost", 9090);

    DataStream<Long> sum = data.map(new MapFunction<String, Tuple2<Long, String>>() {
      public Tuple2<Long, String> map(String s) {
        String[] words = s.split(",");
        return new Tuple2<Long, String>(Long.parseLong(words[0]), words[1]);
      }
    })
        .keyBy(0)
        .flatMap(new StatefulMap());
    sum.writeAsText("/tmp/flinkstate");

    // execute program
    env.execute("State");
  }

  public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long> {
    private transient ValueState<Long> sum;
    private transient ValueState<Long> count;

    public void flatMap(Tuple2<Long, String> input, Collector<Long> out) throws Exception {
      Long currCount = count.value();
      Long currSum = sum.value();

      currCount += 1;
      currSum = currSum + Long.parseLong(input.f1);

      count.update(currCount);
      sum.update(currSum);

      if (currCount >= 10) {
        /* emit sum of last 10 elements */
        out.collect(sum.value());
        /* clear value */
        count.clear();
        sum.clear();
      }
    }

    public void open(Configuration conf) {
      ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>("sum", TypeInformation.of(new TypeHint<Long>() {
      }), 0L);
      sum = getRuntimeContext().getState(descriptor);

      ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>("count", TypeInformation.of(new TypeHint<Long>() {
      }), 0L);
      count = getRuntimeContext().getState(descriptor2);
    }
  }
}


