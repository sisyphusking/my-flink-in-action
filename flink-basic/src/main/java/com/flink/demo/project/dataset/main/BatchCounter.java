package com.flink.demo.project.dataset.main;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * 需求：求出输入元素的总和
 */

public class BatchCounter {

    public static void main(String[] args) throws  Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            //创建累加器
           private IntCounter numLines =  new IntCounter();

            @Override
            public String map(String s) throws Exception {
                // 这里不能使用sum来求和，因为如果并行度大于1，那么会有多个线程来执行，这里sum就不准了，因此需要使用全局累加器
                this.numLines.add(1);
                return s;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //注册累加器
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }
        }).setParallelism(4);

        result.writeAsText("/Users/xavieryin/Desktop/test");

        JobExecutionResult counter = env.execute("counter");
        int num = counter.getAccumulatorResult("num-lines");
        System.out.println("num: "+ num);
    }
}