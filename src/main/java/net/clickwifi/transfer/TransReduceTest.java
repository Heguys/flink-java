package net.clickwifi.transfer;

import net.clickwifi.bean.ClickSource;
import net.clickwifi.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSource())
                .map((MapFunction<Event, Tuple2<String, Long>>) event -> Tuple2.of(event.user, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                        .keyBy(r -> r.f0)
                                .reduce((Tuple2<String, Long> value1, Tuple2<String, Long> value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                                        .keyBy(r -> true)
                                                .reduce((Tuple2<String, Long> value1, Tuple2<String, Long> value2) -> value1.f1 > value2.f1 ? value1 : value2)
                                                        .print();
        env.execute();
    }
}
