package com.flink.demo.project.datastream.windowfunction;


import com.flink.demo.project.datastream.deserializer.ResultDeserializer;
import com.flink.demo.project.datastream.entity.ResultEntity;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 输入的数据类型：{"ReceivedTimeStamp": 1555644803454, "ProjId": 11, "value": {"content": {"cmdcode": "0021001", "data": {} "AppId": 1}
 */
public class CustomProcessWindowFunction {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "localhost:9092");
        //设置kafka offset
        prop.setProperty("auto.offset.reset", "earliest");
        prop.setProperty("group.id", "mytest");

        DataStreamSource<ResultEntity> stream = env.addSource(new FlinkKafkaConsumer011<>("mysqldemo", new ResultDeserializer(), prop));

        //根据appid分组后，统计窗口中的元素个数
        stream.keyBy(new KeySelector<ResultEntity,  String>(){

            @Override
            public String getKey(ResultEntity resultEntity) throws Exception {
                return resultEntity.getAppId().toString();
            }
        }).timeWindow(Time.seconds(10)).process(new ProcessWindowFunction<ResultEntity, String, String, TimeWindow>() {
            //iterable：输入
            //s： key
            Long count = 0L;
            @Override
            public void process(String s, Context context, Iterable<ResultEntity> input, Collector<String> out) throws Exception {
                for (ResultEntity r: input){
                    count++;
                }
                out.collect("window: "+ context.window() + "count: "+ count);
            }
        });

        env.execute("window function demo");

    }


}