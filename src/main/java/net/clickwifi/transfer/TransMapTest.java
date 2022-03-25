package net.clickwifi.transfer;

import net.clickwifi.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L));
        stream.map((MapFunction<Event, String>) event -> event.user).print();
        env.execute();
    }
}
