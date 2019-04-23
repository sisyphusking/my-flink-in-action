package com.flink.demo.project.datastream.main;

import com.flink.demo.project.datastream.source.NoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 把元素广播给所有分区，数据会重复使用
 */
public class StreamBroadcast {

    public static void main(String[] args) throws  Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Long> data = env.addSource(new NoParallelSource()).setParallelism(1);

        //使用broadcast，广播形式，会输出4次，上面设置了并行度
        DataStream<Long> map = data.broadcast().map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据："+ value);
                return value;
            }
        });

        SingleOutputStreamOperator<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        env.execute("stream broadcast");

    }
    
}