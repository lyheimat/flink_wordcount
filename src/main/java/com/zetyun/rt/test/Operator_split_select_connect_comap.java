package com.zetyun.rt.test;

import com.zetyun.rt.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.awt.image.ImageProducer;
import java.util.Collections;

public class Operator_split_select_connect_comap {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> lineDS = env.readTextFile("input");

        SingleOutputStreamOperator<SensorReading> sensorDS = lineDS.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        SplitStream<SensorReading> splitDS = sensorDS.split(new OutputSelector<SensorReading>() {
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemp() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highDS = splitDS.select("high");


        DataStream<SensorReading> lowDS = splitDS.select("low");

        SingleOutputStreamOperator<Tuple2<String, Double>> highPlusDS = highDS.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<String, Double>(sensorReading.getId(), sensorReading.getTemp());
            }
        });

        ConnectedStreams<SensorReading, Tuple2<String, Double>> connectDS = lowDS.connect(highPlusDS);
        SingleOutputStreamOperator<Object> comapDS = connectDS.map(new CoMapFunction<SensorReading, Tuple2<String, Double>, Object>() {
            public Object map1(SensorReading sensorReading) throws Exception {
                return new Tuple2<String, String>(sensorReading.getId(), "healthy");
            }

            public Object map2(Tuple2<String, Double> tuple2) throws Exception {
                return new Tuple3<String, Double, String>(tuple2.f0, tuple2.f1, "warnning");
            }
        });

//        splitDS
//                .select("high", "low")
//                .print("all");

//        comapDS.print("connect_comap");

        highDS.union(lowDS).print("union");

        env.execute();

    }
}
