package com.flink.demo.project.dataset.main;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class BatchMapPartition {

    public static void main(String[] args) throws  Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String>  data = new ArrayList<>();
        data.add("hello xavieryin");
        data.add("fuck world");

        DataSource<String> text = env.fromCollection(data);

        DataSet<String> mapPartition = text.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> collector) throws Exception {

                Iterator<String> it = values.iterator();
                while (it.hasNext()){
                    String next = it.next();
                    String[] split = next.split("\\w+");
                    for (String word: split){
                        collector.collect(word);
                    }
                }
            }

        });
        mapPartition.print();


    }
}