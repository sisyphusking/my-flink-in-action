package com.flink.demo.project.datastream.transformation;

import com.flink.demo.project.datastream.source.NoParallelSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;


/**
 * 实际业务中，数据流中混合了多种类似的数据，多种数据类型的处理逻辑不一样，可以使用split进行切分，然后产生多种输出
 * split经常和select配合使用
 */
public class StreamDemoSplit {


    public static void main(String[] args) throws  Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> data = env.addSource(new NoParallelSource());

        SplitStream<Long> splitOutPut = data.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long aLong) {

                ArrayList<String> output = new ArrayList<>();
                if (aLong %2 ==0){
                    //对偶数进行一个标识
                    output.add("even");
                }else {
                    //对奇数进行一个标识
                    output.add("odd");
                }
                return output;
            }
        });

        //选择一个或者多个切分后的流，这里选择偶数流
        DataStream<Long> even = splitOutPut.select("even");

        // 这里可以选择多个流，可以恢复到原样
        //DataStream<Long> merge = splitOutPut.select("even", "odd");

        even.print().setParallelism(1);

        //merge.print().setParallelism(1);
        env.execute("split stream");
    }
}