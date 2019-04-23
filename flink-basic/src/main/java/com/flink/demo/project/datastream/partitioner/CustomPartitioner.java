package com.flink.demo.project.datastream.partitioner;

import org.apache.flink.api.common.functions.Partitioner;

public class CustomPartitioner  implements Partitioner<Long> {

    //aLong是原始数据，i是分区总数
    @Override
    public int partition(Long aLong, int i) {
        System.out.println("分区总数："+ i);

        //只做了两个分区
        if (aLong%2==0) {
            return 0;
        } else {
            return 1;
        }
    }
}