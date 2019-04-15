package com.qq.welink.project.datastream.main;

import com.qq.welink.project.datastream.partitioner.CustomPartitioner;
import com.qq.welink.project.datastream.source.NoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 根据数字的奇偶性来分区
 */
public class StreamWithPartitioner {

    public static void main(String[] args) throws  Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> data = env.addSource(new NoParallelSource());

        //如果不设置并行度，那么分区数默认和cpu核数一致
        env.setParallelism(2);

        //这里需要将long类型转换成Tuple类型
        DataStream<Tuple1<Long>> map = data.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long aLong) throws Exception {
                return new Tuple1<>(aLong);
            }
        });
        //因为这里的field是索引，所以需要上面转换成Tuple的一步
        //分区之后的数据
        //这里只有两个分区
        DataStream<Tuple1<Long>> partitionData = map.partitionCustom(new CustomPartitioner(), 0);

        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> longTuple1) throws Exception {
                System.out.println("当前线程ID："+ Thread.currentThread().getId()+", value: "+longTuple1);
                return longTuple1.getField(0);
            }
        });


        result.print().setParallelism(1);
        env.execute("stream partitioner");
    }
}