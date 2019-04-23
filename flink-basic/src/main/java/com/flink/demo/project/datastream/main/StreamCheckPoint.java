package com.flink.demo.project.datastream.main;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamCheckPoint {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置每隔一秒钟启动一个检查点
        env.enableCheckpointing(1000);
        //设置模式为exactly once，这是默认模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间至少有500ms的间隔 【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //检查点必须在一分钟内完成，或者被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //一旦flink程序被cancle后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackkend
        //默认是内存这种
        // env.setStateBackend(new MemoryStateBackend());

        //指定存储到hdfs，这种适用于单任务场景，全局的需要修改配置文件
        env.setStateBackend(new FsStateBackend("hdfs://hadoop/xxx"));


        //代码不完整。。。。



    }
    
}