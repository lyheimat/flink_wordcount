package com.zetyun.rt.test;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Rich_Function {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineDS = env.readTextFile("input");

        lineDS.map(new RichMapFunction<String, String>() {

            private ValueState<String> valueState;
            public String map(String s) throws Exception {
                return null;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("aaa", String.class));
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });
    }
}
