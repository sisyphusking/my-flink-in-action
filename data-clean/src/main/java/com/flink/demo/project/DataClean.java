package com.flink.demo.project;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.flink.demo.project.source.MyRedisSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * 创建kafka topic :
 * sh ./kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 5  --topic  allData
 * sh ./kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 5  --topic  allDataClean
 *
 *
 */
public class DataClean {

    public static void main( String[] args )  throws  Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //这里需要设置并行度，一般和kafka的partition数量保持一致
        env.setParallelism(5);

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statabackend 这里我们使用内存

        //指定kafka source
        String topic = "allData";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "127.0.0.1:9092");
        prop.setProperty("group.id", "con1");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), prop);

        //获取kafka中的数据
        // {"dt":"2018-01-01 11:11:11", "countryCode":"US", "data":[{"type": "s1", "score": 0.3, "level":"A"},{"type": "s2", "score": 0.2, "level":"B"}]}
        DataStreamSource<String> data = env.addSource(myConsumer);


        //这个redis是自己实现的sourcefunction，默认并行度是1，这就意味着mapData的并行度是1，而其他算子默认的并行度和cpu核数（8）保持一致，这里就导致有的时候有7个线程是获取不到这个mapData的数据的，所以最终结果会有为空的情况
        //解决办法就是：使用广播broadcast()，将当前算子的数据广播到下游的算子的每一个并行度上

        //最新国家码和大区的映射关系
        DataStream<HashMap<String, String>> mapData = env.addSource(new MyRedisSource()).broadcast();  //注意这里一定要使用广播


        //合并两个流
        DataStream<String> resData = data.connect(mapData).flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {

            //存储国家和大区的映射关系, 开始是由redis中返回的
            private HashMap<String, String> allMap = new HashMap<String, String>();

            // flatMap1中处理的是kafak中的数据
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String dt = jsonObject.getString("dt");
                String countryCode = jsonObject.getString("countryCode");
                //获取大区
                System.out.println("-------"+allMap+"-------");
                String area = allMap.get(countryCode);
                JSONArray jsonArray = jsonObject.getJSONArray("data");

                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject1 = jsonArray.getJSONObject(i);
                    jsonObject1.put("area", area);
                    jsonObject1.put("dt", dt);
                    out.collect(jsonObject1.toJSONString());
                }
            }

            //flatMap2处理的是redis返回的map类型数据
            @Override
            public void flatMap2(HashMap<String, String> value, Collector<String> out) throws Exception {
                this.allMap = value;
            }
        });

        //将数据出入到kafka中

        String outTopic = "allDataClean";
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers", "127.0.0.1:9092");

        outProp.setProperty("transaction.timeout.ms", 60000*15+"");

        //设置exactly once语义的kafka producer，保证向kafka写入的数据仅有一次
        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<>(outTopic, new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), outProp, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);

        resData.addSink(producer);

        env.execute("data clean");

    }
}
