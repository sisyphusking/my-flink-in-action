package com.flink.demo.project.datastream.windowfunction;


import com.flink.demo.project.datastream.deserializer.ResultDeserializer;
import com.flink.demo.project.datastream.entity.ResultEntity;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 输入的数据类型：{"ReceivedTimeStamp": 1555644803454, "ProjId": 11, "value": {"content": {"cmdcode": "0021001", "data": {} "AppId": 1}
 */
public class CustomReduceFunction {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "localhost:9092");
        //设置kafka offset
        prop.setProperty("auto.offset.reset", "earliest");
        prop.setProperty("group.id", "mytest");

        DataStreamSource<ResultEntity> stream = env.addSource(new FlinkKafkaConsumer011<>("mysqldemo", new ResultDeserializer(), prop));

        //将每一次滚动的窗口中的所有元素的AppId字段进行相加
        DataStream<ResultEntity> reduce = stream.timeWindowAll(Time.minutes(1)).reduce(new ReduceFunction<ResultEntity>() {
            @Override
            public ResultEntity reduce(ResultEntity resultEntity, ResultEntity t1) throws Exception {

                ResultEntity test = new ResultEntity();
                test.setAppId(resultEntity.getAppId()+t1.getAppId());
                //打印出每加一个元素值的和
                //System.out.println("appid totol num: "+ test.getAppId());
                return test;
            }
        });

        reduce.map(new MapFunction<ResultEntity, Long>() {
            @Override
            public Long map(ResultEntity resultEntity) throws Exception {
                System.out.println("appid num: "+ resultEntity.getAppId());
                return resultEntity.getAppId();
            }
        });

        env.execute("window function demo");

    }


}