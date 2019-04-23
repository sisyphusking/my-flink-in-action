package com.flink.demo.project.datastream.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义一个没有并行度的source
 */

//这里需要指定sourceFunction的泛型
public class NoParallelSource implements SourceFunction<Long> {

    private long count = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext sourceContext) throws Exception {

        while(isRunning){
            // collect是传递出去的数据
            sourceContext.collect(count);
            count++;
            //等待一秒
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}