package com.qq.welink.project.datastream.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * RichParallelSource会额外提供open和close方法，如果close中需要获取其他资源链接，可以在open中声明，close中关闭
 */
//再次注意需要指定类型，不然会报错
//Caused by: org.apache.flink.api.common.functions.InvalidTypesException: Type of TypeVariable 'OUT' in 'interface
// org.apache.flink.streaming.api.functions.source.ParallelSourceFunction' could not be determined. This is most likely a type erasure problem.
// The type extraction currently supports types with generic variables only in cases where all variables in the return
// type can be deduced from the input type(s). Otherwise the type has to be specified explicitly using type information.
public class RichParallelSource  extends RichParallelSourceFunction<Long> {


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

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open.....");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        System.out.println("close....");
        super.close();
    }
}