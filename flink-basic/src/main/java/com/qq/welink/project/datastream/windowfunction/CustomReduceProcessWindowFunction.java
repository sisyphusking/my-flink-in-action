package com.qq.welink.project.datastream.windowfunction;


import com.qq.welink.project.datastream.deserializer.ResultDeserializer;
import com.qq.welink.project.datastream.entity.ResultEntity;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 输入的数据类型：{"ReceivedTimeStamp": 1555644803454, "ProjId": 11, "value": {"content": {"cmdcode": "0021001", "data": {} "AppId": 1}
 */
public class CustomReduceProcessWindowFunction {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "localhost:9092");
        //设置kafka offset
        prop.setProperty("auto.offset.reset", "earliest");
        prop.setProperty("group.id", "mytest");

        DataStreamSource<ResultEntity> stream = env.addSource(new FlinkKafkaConsumer011<>("mysqldemo", new ResultDeserializer(), prop));

        //ReduceFunction 与 ProcessWindowFunction 结合以返回窗口中的最小appid值以及窗口的开始时间。
        //可以使用传统WindowFunction而不是ProcessWindowFunction进行增量窗口聚合。
        stream.keyBy(new KeySelector<ResultEntity, String>() {

            @Override
            public String getKey(ResultEntity resultEntity) throws Exception {
                return resultEntity.getAppId().toString();
            }
        }).timeWindow(Time.seconds(10)).reduce(new MyReduceFunction(), new MyWindowFunction());

        env.execute("window function demo");

    }


    //统计出最小appid
    private static class MyReduceFunction implements ReduceFunction<ResultEntity> {

        public ResultEntity reduce(ResultEntity r1, ResultEntity r2) {
            return r1.getAppId() > r2.getAppId() ? r2 : r1;
        }
    }

    private static class MyWindowFunction
            implements WindowFunction<ResultEntity, Tuple2<Long, ResultEntity>, String, TimeWindow> {

        public void apply(String key,
                          TimeWindow window,
                          Iterable<ResultEntity> minReadings,
                          Collector<Tuple2<Long, ResultEntity>> out) {
            ResultEntity min = minReadings.iterator().next();
            out.collect(new Tuple2<Long, ResultEntity>(window.getStart(), min));
        }

    }

}
