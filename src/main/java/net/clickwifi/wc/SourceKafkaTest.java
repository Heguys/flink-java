package net.clickwifi.wc;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SourceKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "alikafka-pre-cn-oew1q0q1q001-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-oew1q0q1q001-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-oew1q0q1q001-3-vpc.alikafka.aliyuncs.com:9092");
        properties.setProperty("group.id", "flink-test-heguys");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> stream = env.addSource(new
                FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties
        ));
        stream.print("Kafka");
        env.execute();
    }
} 
