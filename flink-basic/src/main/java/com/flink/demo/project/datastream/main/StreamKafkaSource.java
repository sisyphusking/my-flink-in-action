package com.flink.demo.project.datastream.main;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class StreamKafkaSource {

    public static void main(String[] args) throws  Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "t1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "127.0.0.1:9092");
        prop.setProperty("group.id", "con1");


        /**
         * env checkpoint配置
         */
        //为了支持容错，需要开启checkpoint，每10s checkpoint一次， kafka consumer会定期把kafka的offset信息还有其他算子的信息保存起来，
        // 当job失败重启的时候，flink会从最近一次的checkpoint中进行恢复数据，重新消费kafka中的数据
        env.enableCheckpointing(10000);
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


        // 设置statabackend
        //指定存储到hdfs，这种适用于单任务场景，全局的需要修改配置文件
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop/xxx"));


        /**
         * kafka consumer设置
         */
        //默认记忆上一次消费的位置，下次重新启动后，从上一次消费的位置开始
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), prop);

        //默认消费策略是读取上一次保存的offset信息，如果是应用第一次启动，读取不到上次的offset信息，则会根据这个参数的auto.offset.reset的值来进行消费数据
        consumer.setStartFromGroupOffsets();

        DataStreamSource<String> text = env.addSource(consumer);

        text.print().setParallelism(1);

        env.execute("stream  kafka");


    }
    
}