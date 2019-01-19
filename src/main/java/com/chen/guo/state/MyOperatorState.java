package com.chen.guo.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * An example of the managed operator state
 */
public class MyOperatorState implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {
  private final int threshold;
  //A list of serializable objects
  private transient ListState<Tuple2<String, Integer>> checkpointedState;

  //The temporary location for the elements before putting them into the list
  private List<Tuple2<String, Integer>> bufferedElements;

  public MyOperatorState(int threshold) {
    this.threshold = threshold;
    this.bufferedElements = new ArrayList<Tuple2<String, Integer>>();
  }

  /**
   * from SinkFunction interface
   * <p>
   * To write any value into the Sink
   *
   * @param value
   * @throws Exception
   */
  public void invoke(Tuple2<String, Integer> value) throws Exception {
    bufferedElements.add(value);
    if (bufferedElements.size() == threshold) {
      for (Tuple2<String, Integer> element : bufferedElements) {
        /**
         * send it to the sink here!!!
         * such as write into text files, or other sinks
         */
      }
      bufferedElements.clear();
    }
  }

  /**
   * from CheckpointedFunction interface
   *
   * @param context
   * @throws Exception
   */
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    checkpointedState.clear();
    for (Tuple2<String, Integer> element : bufferedElements) {
      checkpointedState.add(element);
    }
  }

  /**
   * from CheckpointedFunction interface
   *
   * @param context
   * @throws Exception
   */
  public void initializeState(FunctionInitializationContext context) throws Exception {
    ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<Tuple2<String, Integer>>("buffered-elements",
        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

    checkpointedState = context.getOperatorStateStore()
        /**
         * .getUninonListState(descriptor): broadcast the entire state to all reduce operators
         * .getListState(descriptor): shuffle an evenly redistributed state partition to a reduce operator
         */
        .getListState(descriptor);

    //Check if we are recovering from failure or not
    if (context.isRestored()) {
      for (Tuple2<String, Integer> element : checkpointedState.get()) {
        bufferedElements.add(element);

      }
    }
  }
}


