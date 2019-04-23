package com.flink.demo.project.datastream.main;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamIncreAgg {


    public static void main(String[] args)  throws Exception{

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> text = env.socketTextStream("localhost", 9000, "\n");
        DataStream<Tuple2<Integer, Integer>> map = text.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String s) throws Exception {
                return new Tuple2<>(1, Integer.parseInt(s));
            }
        });

        map.keyBy(0)
                .timeWindow(Time.seconds(5))
                //增量聚合，窗口中每进入一条数据，就进行一次计算
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
                        System.out.println("执行reduce操作："+t1+"， "+t2);
                        return new Tuple2<>(t1.f0, t1.f1+t2.f1);
                    }
                }).print();

        env.execute("reduce window count");

    }
}