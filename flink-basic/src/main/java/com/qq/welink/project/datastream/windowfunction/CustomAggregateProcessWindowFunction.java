package com.qq.welink.project.datastream.windowfunction;


import com.qq.welink.project.datastream.deserializer.ResultDeserializer;
import com.qq.welink.project.datastream.entity.ResultEntity;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class CustomAggregateProcessWindowFunction {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "localhost:9092");
        //设置kafka offset
        prop.setProperty("auto.offset.reset", "earliest");
        prop.setProperty("group.id", "mytest");

        DataStreamSource<ResultEntity> stream = env.addSource(new FlinkKafkaConsumer011<>("mysqldemo", new ResultDeserializer(), prop));

        //AggregateFunction 与 ProcessWindowFunction结合来计算平均值，并将平均值输出，进行增量计算

        stream.keyBy(new KeySelector<ResultEntity, String>() {

            @Override
            public String getKey(ResultEntity resultEntity) throws Exception {
                return resultEntity.getAppId().toString();
            }
        }).timeWindow(Time.seconds(5)).aggregate(new AverageAggregate(), new MyProcessWindowFunction()).print();


        env.execute("window function demo");

    }


    private static class AverageAggregate
            implements AggregateFunction<ResultEntity, Tuple2<Long, Long>, Long> {

        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(ResultEntity value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.getTimeStamp(), accumulator.f1 + 1L);
        }

        @Override
        public Long getResult(Tuple2<Long, Long> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
            return null;
        }
    }

    //第一个值是输入，第二个值是输出类型，第三个值是key, 第四个值是timewindow
    private static class MyProcessWindowFunction extends ProcessWindowFunction<Long, String, String, TimeWindow> {


        @Override
        public void process(String s, Context context, Iterable<Long> input, Collector<String> out) throws Exception {

            Long next = input.iterator().next();
            out.collect("avg: "+ next);

        }
    }

}
