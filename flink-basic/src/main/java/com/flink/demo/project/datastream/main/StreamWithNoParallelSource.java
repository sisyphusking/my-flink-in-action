package com.flink.demo.project.datastream.main;

import com.flink.demo.project.datastream.source.NoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 * Hello world!
 *
 */
public class StreamWithNoParallelSource {

    public static void main( String[] args ) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取自定义的并行度为1的数据源
        //这里datasource也需要指定泛型，也就是输出的数据的类型
        DataStreamSource<Long> data = env.addSource(new NoParallelSource());


        //针对sourceFunciton并行度只能为1，如果像下面这样设置成2，会报错
        // DataStreamSource<Long> data = env.addSource(new NoParallelSource()).setParallelism(2);

        DataStream<Long> map = data.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据："+ value);
                return value;
            }
        });

        // map使用的是timeWindowAll
        // keyby使用的是timeWindow

        //窗口每两秒滑动一次，然后对窗口中的数据进行求和
        SingleOutputStreamOperator<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        env.execute("stream no paralle source");
    }
}
