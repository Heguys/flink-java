package net.clickwifi.transfer;

import net.clickwifi.bean.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L));
        stream.flatMap((Event event, Collector<String> out) -> {
            if(event.user.equals("Mary")){
                out.collect(event.user);
            }else if (event.user.equals("Bob")){
                out.collect(event.user);
            }
            out.collect(event.url);
        }).returns(Types.STRING).print();
        env.execute();
    }
}
