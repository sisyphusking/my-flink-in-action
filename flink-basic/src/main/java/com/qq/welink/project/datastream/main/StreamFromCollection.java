package com.qq.welink.project.datastream.main;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * Hello world!
 *
 */
public class StreamFromCollection {

    public static void main( String[] args ) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Integer> data = new ArrayList<>();
        data.add(10);
        data.add(20);
        data.add(30);

        //指定数据源，fromcolleciotn：从集合中创建数据流，集合中所有元素必须是同一种类型
        DataStreamSource<Integer> collecitonData = env.fromCollection(data);

        //使用map对数据进行处理
        DataStream<Integer> map = collecitonData.map(new MapFunction<Integer, Integer>() {
            //将输入源中的数加1
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer +1;
            }
        });

        //打印出datastream，并且将并行度设置为1
        map.print().setParallelism(1);

        env.execute("my demo project");

    }
}
