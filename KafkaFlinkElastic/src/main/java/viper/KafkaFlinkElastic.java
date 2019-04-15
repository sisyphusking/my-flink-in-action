package viper;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.http.HttpHost;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.util.*;

/**
 * kafka输入的数据类型：192.168.1.1，test
 *
 */

public class KafkaFlinkElastic {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = readFromKafka(env);
        stream.print();
        writeToElastic(stream);
        env.execute("Viper Flink!");
    }

    public static DataStream<String> readFromKafka(StreamExecutionEnvironment env) {

//        env.enableCheckpointing(5000);
        //设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        DataStream<String> stream = env.addSource(
                new FlinkKafkaConsumer011<String>("mykafka", new SimpleStringSchema(), properties));
        return stream;
    }

    public static void writeToElastic(DataStream<String> input) {

        try {

            List<HttpHost> address =new ArrayList<>();
            address.add(new HttpHost("127.0.0.1", 9200 ,"http"));

            ElasticsearchSinkFunction<String> indexLog = new ElasticsearchSinkFunction<String>() {

                public IndexRequest createIndexRequest(String element) {
                    String[] logContent = element.trim().split(",");
                    Map<String, String> esJson = new HashMap<>();
                    esJson.put("IP", logContent[0]);
                    esJson.put("info", logContent[1]);

                    return Requests
                            .indexRequest()
                            .index("index")
                            .type("type")
                            .source(esJson);
                }

                @Override
                public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                    indexer.add(createIndexRequest(element));
                }
            };

            ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(address, indexLog);
            esSinkBuilder.setBulkFlushMaxActions(1);

            input.addSink(esSinkBuilder.build());

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}
