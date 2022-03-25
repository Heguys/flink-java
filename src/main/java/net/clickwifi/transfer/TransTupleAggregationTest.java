package net.clickwifi.transfer;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransTupleAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );
//        stream.keyBy(r -> r.f0).sum(1).print();
//        stream.keyBy((Tuple2<String, Integer> r) -> r.f0).sum(1).print();
        stream.keyBy((KeySelector<Tuple2<String, Integer>, String>) r -> r.f0).sum("f1").print();
        env.execute();
    }
}
