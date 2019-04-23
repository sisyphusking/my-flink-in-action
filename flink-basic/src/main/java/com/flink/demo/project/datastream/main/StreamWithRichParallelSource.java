package com.flink.demo.project.datastream.main;

import com.flink.demo.project.datastream.source.RichParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamWithRichParallelSource {

    public static void main(String[] args)  throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //这里使用的是并行的source，如果不指定并行度，那么默认使用的是并行度和cpu核数保持一致
        //会优先执行open方法
        DataStreamSource<Long> data = env.addSource(new RichParallelSource()).setParallelism(3);

        DataStream<Long> map = data.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据："+ value);
                return value;
            }
        });

        SingleOutputStreamOperator<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        env.execute("stream rich paralle source");
    }
}
