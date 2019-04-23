package com.flink.demo.project.datastream.transformation;

import com.flink.demo.project.datastream.source.NoParallelSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamDemoFilter {

    public static void main(String[] args)  throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> data = env.addSource(new NoParallelSource());

        DataStream<Long> map = data.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                System.out.println("原始数据：" + aLong);
                return aLong;
            }
        });


        //执行filter方法，filter(）返回的是boolean类型，满足条件的会被留下来，也就是aLong %2 ==0
        DataStream<Long> filter = map.filter(new FilterFunction<Long>() {

            // 把所有奇数都过滤掉
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong %2 ==0;
            }
        });


        SingleOutputStreamOperator<Long> filterdData = filter.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                System.out.println("过滤后的数据："+ aLong);
                return aLong;
            }
        });


        DataStream<Long> sum = filterdData.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        env.execute("filter stream");

    }
}