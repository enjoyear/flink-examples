package com.chen.guo.state.broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class EmpCount {

  /**
   * The table which contains the employee data that needs to be exclude
   * The table has two columns:
   * Emp_id, Emp_name
   */
  public static final MapStateDescriptor<String, String> excludeEmpDescriptor =
      new MapStateDescriptor<String, String>("exclude_employ", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);


  public static void main(String[] args) throws Exception {
    final String employeeData = EmpCount.class.getResource("/state/broadcast/currentEmployees.txt").toURI().getPath();

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> excludeEmp = env.socketTextStream("localhost", 9090);
    BroadcastStream<String> broadcastStreamToExclude = excludeEmp.broadcast(excludeEmpDescriptor);

    DataStream<Tuple2<String, Integer>> employees = env.readTextFile(employeeData)
        .map(new MapFunction<String, Tuple2<String, String>>() {
          public Tuple2<String, String> map(String value) {
            // return <department, full record>
            // e.g. {(Purchase), (AXPM175755,Nana,Developer,Purchase,GH67D)}
            return new Tuple2<String, String>(value.split(",")[3], value);
          }
        })
        .keyBy(0)
        //To connect a non-broadcast stream with a broadcast stream
        //it will return a BroadcastConnectedStream
        .connect(broadcastStreamToExclude)
        .process(new ExcludeEmp());

    employees.writeAsText("/tmp/broadcast_state", FileSystem.WriteMode.OVERWRITE);
    env.execute("Broadcast Example");
  }

  /**
   * If the stream is keyed, extend the KeyedBroadcastProcessFunction
   * otherwise, extend the BroadcastProcessFunction
   * <p>
   * Generic Types:
   * 1st type: key type of the input keyed stream
   * 2nd type: input type of the keyed non-broadcast side
   * 3rd type: input type of the broadcast side
   * 4th type: output type of the operator
   */
  public static class ExcludeEmp extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, String, Tuple2<String, Integer>> {
    private transient ValueState<Integer> countState;

    /**
     * Process the elements from the non-broadcast side
     */
    public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
      Integer currCount = countState.value();
      // get card_id of current transaction
      final String cId = value.f1.split(",")[0];

      //Only reading the broadcast state. No writing allowed.
      for (Map.Entry<String, String> cardEntry : ctx.getBroadcastState(excludeEmpDescriptor).immutableEntries()) {
        final String excludeId = cardEntry.getKey();
        if (cId.equals(excludeId))
          //Return immediately if there is no records from the broadcast side
          return;
      }

      countState.update(currCount + 1);       // dept    , current sum
      out.collect(new Tuple2<String, Integer>(value.f0, currCount + 1));
    }

    /**
     * Process the elements from the broadcast side
     *
     * @param empData a record from the broadcast side
     */
    public void processBroadcastElement(String empData, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
      //Extract the key from the broadcast stream
      String id = empData.split(",")[0];
      //Need to update the broadcast state for each new coming broadcast record
      ctx.getBroadcastState(excludeEmpDescriptor).put(id, empData);
    }

    public void open(Configuration conf) {
      ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<Integer>("", BasicTypeInfo.INT_TYPE_INFO, 0);
      countState = getRuntimeContext().getState(desc);
    }
  }
}

