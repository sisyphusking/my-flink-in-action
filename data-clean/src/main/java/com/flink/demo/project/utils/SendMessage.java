package com.flink.demo.project.utils;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class SendMessage {

    public static void main(String[] args)  throws  Exception{

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "127.0.0.1:9092");
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());

        String topic = "allData";
        //创建producer 链接
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        while (true){
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("dt", getCurrentTime());
            jsonObject.put("countryCode", getCountryCode());

            JSONArray jsonArray = new JSONArray();
            JSONObject jsonObject1 = new JSONObject();
            JSONObject jsonObject2 = new JSONObject();
            jsonObject1.put("type", getRandomType());
            jsonObject1.put("level", getRandomLevel());
            jsonObject2.put("type", getRandomType());
            jsonObject2.put("level", getRandomLevel());
            jsonArray.add(jsonObject1);
            jsonArray.add(jsonObject2);

            jsonObject.put("data", jsonArray.toString());

            String message = jsonObject.toJSONString();
            System.out.println(message);

            producer.send(new ProducerRecord<>(topic, message));
            Thread.sleep(2000);
        }
    }

    public static String getCurrentTime(){

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    public static String getCountryCode(){
        String[] types = {"US","TW","HK","PK","KW","SA","IN"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String getRandomType(){
        String[] types = {"s1","s2","s3","s4","s5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static double getRandomScore(){
        double[] types = {0.1,0.2,0.3};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    public static String getRandomLevel(){
        String[] types = {"A","B","C","D","E"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }
}