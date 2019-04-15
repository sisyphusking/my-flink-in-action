package com.qq.welink.project.datastream.main;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.table.descriptors.Kafka;

import java.util.Properties;


public class StreamKafkaSink {

    public static void main(String[] args) throws  Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


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

        String topic = "t1";
        String brokerList = "127.0.0.1:9092";

        DataStreamSource<String> text = env.socketTextStream("localhost", 9000, "\n");

        //常规的kafka生产者类型
        //FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());

        /**
         * 第一种解决方案：设置FlinkKafkaProducer011里面事务超时时间
         */

        //有一个关键性的设置，参考这里：https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html
        //Kafka brokers by default have transaction.max.timeout.ms set to 15 minutes.
        // This property will not allow to set transaction timeouts for the producers larger than it’s value.
        // FlinkKafkaProducer011 by default sets the transaction.timeout.ms property in producer config to 1 hour,
        // thus transaction.max.timeout.ms should be increased before using the Semantic.EXACTLY_ONCE mode

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //设置事务最大超时时间，因为上面的原因这里我们不能大于kafka默认的最大超时时间15分钟，所以这里一定要设置， 如果设置可能会导致程序起不起来
        prop.setProperty("transaction.timeout.ms", 60000*15+"");

        //设置exactly once语义的kafka producer，保证向kafka写入的数据仅有一次
        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<>(topic, new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), prop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);


        /**
         * 第二种解决方案：设置kafka的超时时间
         */
        //直接修改kafka配置文件(conf 目录下的server.properties)中的 transaction.max.timeout.ms = 360000  (改成4小时)





        text.addSink(producer);

        env.execute("stream  kafka");


    }
    
}