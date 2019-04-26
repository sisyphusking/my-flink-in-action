package com.flink.demo.project.datastream.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class EsSink {
    
    public static  void writeToElastic (DataStream<Tuple4<Long, Long, Long, String>> input){

        try {
            List<HttpHost> address =new ArrayList<>();
            address.add(new HttpHost("localhost", 9200 ,"http"));

            ElasticsearchSinkFunction<Tuple4<Long, Long, Long, String>> indexLog = new ElasticsearchSinkFunction<Tuple4<Long, Long, Long, String>>() {


                public IndexRequest createIndexRequest(Tuple4<Long, Long, Long, String> element) {
                    Long appid = element.f0;
                    Long projectId = element.f1;
                    Long timeStamp = element.f2;
                    String value = element.f3;
                    Map<String, String> esJson = new HashMap<>();
                    esJson.put("appId", appid.toString());
                    esJson.put("projectId", projectId.toString());
                    esJson.put("timestamp", timeStamp.toString());
                    esJson.put("value", value);

                    return Requests
                            .indexRequest()
                            .index("my-index")
                            .type("my-type")
                            .source(esJson);
                }

                @Override
                public void process(Tuple4<Long, Long, Long, String> element, RuntimeContext ctx, RequestIndexer indexer) {
                    indexer.add(createIndexRequest(element));
                }
            };

            ElasticsearchSink.Builder<Tuple4<Long, Long, Long, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(address, indexLog);
            esSinkBuilder.setBulkFlushMaxActions(1);

            input.addSink(esSinkBuilder.build());

        } catch (Exception e) {

            e.printStackTrace();
        }


    }
}