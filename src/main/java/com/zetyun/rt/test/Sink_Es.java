package com.zetyun.rt.test;

import com.zetyun.rt.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class Sink_Es {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineDS = env.readTextFile("input");
        SingleOutputStreamOperator<SensorReading> sensorDS = lineDS.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        //es httpHosts设置
        ArrayList<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        sensorDS.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());


    }

    private static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            HashMap<String, String> map = new HashMap<String, String>();
            map.put("id", sensorReading.getId());
            map.put("ts", sensorReading.getTs().toString());
            map.put("temp", sensorReading.getTemp().toString());

            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("_doc")
                    .id(sensorReading.getId())
                    .source(map);

            requestIndexer.add(indexRequest);
        }
    }
}
