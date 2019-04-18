package com.qq.welink.project.dataset.main;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 需求：求出输入元素的总和
 */

public class BatchDIsCache {

    public static void main(String[] args) throws  Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从hdfs或者本地读取缓存文件
        env.registerCachedFile("/Users/xavieryin/Documents/flink/myflinkdemo/src/main/java/com/qq/welink/project/data/test.txt", "a.txt");

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            private ArrayList<String>  dataList = new  ArrayList<String>();

            @Override
            public String map(String s) throws Exception {

                return s + dataList;
            }

            //open方法只会运行一次
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //使用文件
                File file = getRuntimeContext().getDistributedCache().getFile("a.txt");
                List<String> lines = FileUtils.readLines(file);
                for (String line: lines){
                    this.dataList.add(line);
                    System.out.println("line: "+line);
                }
            }
        }).setParallelism(4);

        result.print();
    }
}