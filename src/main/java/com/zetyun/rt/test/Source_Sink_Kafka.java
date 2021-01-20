package com.zetyun.rt.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Source_Sink_Kafka {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "1111");
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:9092");

        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("source", new SimpleStringSchema(), prop));
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] arr = s.split(" ");
                for (String fields : arr) {
                    collector.collect(new Tuple2<String, Integer>(fields, 1));
                }
            }
        });

        Properties prop_sink = new Properties();
        prop_sink.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:9092");

        wordToOneDS
                .keyBy(0)
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    public String map(Tuple2<String, Integer> tuple2) throws Exception {
                        return tuple2.f0 + tuple2.f1;
                    }
                })
                .addSink(new FlinkKafkaProducer011<String>("sink", new SimpleStringSchema(), prop_sink));
    }
}
