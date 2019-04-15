package com.qq.welink.project.datastream.transformation;

import com.qq.welink.project.datastream.source.NoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class StreamDemoConnect {


    public static void main(String[] args) throws  Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> data1 = env.addSource(new NoParallelSource());

        DataStreamSource<Long> data2 = env.addSource(new NoParallelSource());

        //将long型的流转成string类型的流
        DataStream<String> dataString = data2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "string_" + aLong;
            }
        });

        // 将不同数据类型的数据流进行连接，数量只能是两个，
        ConnectedStreams<Long, String> connect = data1.connect(dataString);


        //要分别实现map1和map2方法
        SingleOutputStreamOperator<Object> connectMap = connect.map(new CoMapFunction<Long, String, Object>() {


            @Override
            public Object map1(Long aLong) throws Exception {
                return aLong+10L;
            }

            @Override
            public Object map2(String s) throws Exception {
                return s;
            }
        });

        connectMap.print().setParallelism(1);
        env.execute("connetion stream");
    }
}