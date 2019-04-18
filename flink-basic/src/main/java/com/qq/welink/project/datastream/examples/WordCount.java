package com.qq.welink.project.datastream.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Long>> dataStream = env
                // source
                .socketTextStream("localhost", 9000)
                // flatmap DataStream->DataStream 将读入的一份数据，转换成0到n个。这里就是拆分。
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String [] words = s.split(" ");
                        for(String word : words){
                            collector.collect(word);
                        }
                    }
                })
                // map  DataStream->DataStream 读入一份，转换成一份，这里是组装成tuple对
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        return new Tuple2<String, Long>(s, 1l);
                    }
                })
                // keyBy DataStream->KeyedStream 逻辑上将数据根据key进行分区，保证相同的key分到一起。默认是hash分区
                .keyBy(0)
                // window
                .timeWindow(Time.seconds(2))
                // sum WindowedStream->DataStream 聚合窗口内容。另外还有min,max等
                .sum(1);
        //sink
        dataStream.print();
        env.execute("Word Count!");


    }

}