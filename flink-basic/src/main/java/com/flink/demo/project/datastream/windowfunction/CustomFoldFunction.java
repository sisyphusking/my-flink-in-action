package com.flink.demo.project.datastream.windowfunction;


import com.flink.demo.project.datastream.deserializer.ResultDeserializer;
import com.flink.demo.project.datastream.entity.ResultEntity;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 输入的数据类型：{"ReceivedTimeStamp": 1555644803454, "ProjId": 11, "value": {"content": {"cmdcode": "0021001", "data": {} "AppId": 1}
 */

public class CustomFoldFunction {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "localhost:9092");
        //设置kafka offset
        prop.setProperty("auto.offset.reset", "earliest");
        prop.setProperty("group.id", "mytest");

        DataStreamSource<ResultEntity> stream = env.addSource(new FlinkKafkaConsumer011<>("mysqldemo", new ResultDeserializer(), prop));

        //将每一次滚动的窗口中的所有元素的AppId的值相加，然后在加上这个初始化值init，输出
        Long init=100L;
        DataStream<Long> fold = stream.timeWindowAll(Time.seconds(30)).fold(init, new FoldFunction<ResultEntity, Long>() {
            @Override
            public Long fold(Long r, ResultEntity o) throws Exception {
                return r+ o.getAppId();
            }
        });

        fold.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {

                System.out.println("fold value: "+ aLong);
                return aLong;
            }
        });

        env.execute("window function demo");

    }


}