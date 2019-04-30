package com.flink.demo.project.datastream.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.sql.Types;
import java.util.Properties;

public class SinkJdbc {

    private static final String TOPIC = "tracking";
    private static final String KAFKA_BOOTSTRAP = "192.168.1.4:9092";
    private static final int JDBC_BATCH_SIZE = 200;
    private static final String JDBC_USER = "root";
    private static final String JDBC_PASSWORD = "root";
    private static final String JDBC_DB = "stream";
    private static final String JDBC_TABLE = "tracking";
    private static final String JDBC_HOST = "localhost";


    public static void main(String[] args)  throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new FlinkKafkaConsumer010<>("tracking", new SimpleStringSchema(), new Properties()))
                .setParallelism(10);
                //这里使用flink jdbc
                //参考：https://github.com/hey-johnnypark/apache-flink-jdbc-streaming/blob/master/src/main/java/TrackingEventConsumer.java
                // .writeUsingOutputFormat(createJDBCSink())
        env.execute();
    }

    //flink-jdbc模块中可以直接使用jdbc进行输出
    private static JDBCOutputFormat createJDBCSink() {
        return JDBCOutputFormat.buildJDBCOutputFormat()
                .setDBUrl(String.format("jdbc:mysql://%s/%s", JDBC_HOST, JDBC_DB))
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername(JDBC_USER)
                .setPassword(JDBC_PASSWORD)
                .setQuery(String.format("insert into %s (id, message) values (?,?)", JDBC_TABLE))
                .setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR})
                .setBatchInterval(JDBC_BATCH_SIZE)
                .finish();
    }

}