package com.zetyun.rt.test;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Source_Collection_file {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> collectionDS = env.fromCollection(
                Arrays.asList(
                        new Tuple2<String, Integer>("a", 1),
                        new Tuple2<String, Integer>("b", 3),
                        new Tuple2<String, Integer>("a", 5),
                        new Tuple2<String, Integer>("c", 2),
                        new Tuple2<String, Integer>("b", 4)
                )
        );

        collectionDS.keyBy(0)
                .sum(1)
                .print();

        DataStreamSource<String> fileDS = env.readTextFile("input");

        env.execute();
    }
}
