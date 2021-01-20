package com.zetyun.rt.test;

import com.zetyun.rt.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class Sink_Redis {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineDS = env.readTextFile("input");
        SingleOutputStreamOperator<SensorReading> sensorDS = lineDS.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("server101")
                .setPort(6379)
                .build();
        sensorDS.addSink(new RedisSink<SensorReading>(config, new MyRedisMapper()));

    }

    private static class MyRedisMapper implements RedisMapper<SensorReading> {

        //保存到redis的命令，存成哈希表
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor");
        }

        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getId();
        }

        public String getValueFromData(SensorReading sensorReading) {
            return sensorReading.getTemp().toString();
        }
    }
}
