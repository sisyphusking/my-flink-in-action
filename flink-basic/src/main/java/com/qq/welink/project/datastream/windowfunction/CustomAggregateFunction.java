package com.qq.welink.project.datastream.windowfunction;


import com.qq.welink.project.datastream.deserializer.ResultDeserializer;
import com.qq.welink.project.datastream.entity.ResultEntity;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 输入的数据类型：{"ReceivedTimeStamp": 1555644803454, "ProjId": 11, "value": {"content": {"cmdcode": "0021001", "data": {} "AppId": 1}
 */
public class CustomAggregateFunction {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "localhost:9092");
        //设置kafka offset
        prop.setProperty("auto.offset.reset", "earliest");
        prop.setProperty("group.id", "mytest");

        DataStreamSource<ResultEntity> stream = env.addSource(new FlinkKafkaConsumer011<>("mysqldemo", new ResultDeserializer(), prop));
        //计算窗口中元素的时间戳的平均值
        // IN:输入类型，ACC：累加器类型， OUT: 输出类型
        DataStream<Long> aggregate = stream.timeWindowAll(Time.seconds(5)).aggregate(new AggregateFunction<ResultEntity, Tuple2<Long, Long>, Long>() {

            //创建累加器，初始化都为0
            @Override
            public Tuple2<Long, Long> createAccumulator() {
                return new Tuple2<>(0L, 0L);
            }

            //第一个参数：输入，第二个参数：累加器
            //将每一个输入的timestamp与累加器相加，作为第一个参数，后面的参数是统计个数，因此是每一次都加1
            @Override
            public Tuple2<Long, Long> add(ResultEntity value, Tuple2<Long, Long> accumulator) {
                return new Tuple2<>(accumulator.f0 + value.getTimeStamp(), accumulator.f1 + 1);
            }

            //输入是累加器, 返回结果就是输出
            //统计平均值
            @Override
            public Long getResult(Tuple2<Long, Long> accumulator) {
                return accumulator.f0 / accumulator.f1;
            }

            //将累加器进行合并
            //只有在当窗口合并的时候调用,合并2个容器
            //在没有其他窗口进行合并的情况下，如果将返回设置为null, 累加计算的结果也正确。
            @Override
            public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                //return null;
            }
        });

        aggregate.print();
        env.execute("window function demo");

    }


}