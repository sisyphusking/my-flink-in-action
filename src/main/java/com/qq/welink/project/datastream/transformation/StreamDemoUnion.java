package com.qq.welink.project.datastream.transformation;

import com.qq.welink.project.datastream.source.NoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamDemoUnion {

    public static void main(String[] args) throws  Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> data1 = env.addSource(new NoParallelSource());

        DataStreamSource<Long> data2 = env.addSource(new NoParallelSource());

        //合并两个数据流，且每个数据流中的数据类型要保持一致
        //union可以连接多个流
        DataStream<Long> data = data1.union(data2);

        DataStream<Long> map = data.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据："+ value);
                return value;
            }
        });

        SingleOutputStreamOperator<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        env.execute("stream union");
    }
    
}