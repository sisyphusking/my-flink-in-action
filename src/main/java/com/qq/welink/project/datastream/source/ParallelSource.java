package com.qq.welink.project.datastream.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 *
 */
public class ParallelSource implements ParallelSourceFunction<Long> {


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