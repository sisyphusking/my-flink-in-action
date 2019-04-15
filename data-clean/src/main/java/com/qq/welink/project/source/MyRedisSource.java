package com.qq.welink.project.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.log4j.spi.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * 在redis中进行数据格式化，执行以下命令
 * hset areas AREA_US US
 * hset areas AREA_AR PK,KW,SA
 * hset areas AREA_IN IN
 * hset areas AREA_HH TW,HK
 *
 * 查询结果：hgetall areas
 *
 * 在redis中保存的是国家和大区的关系
 * 需要把大区和国家的对应关系组装成java的hashmap
 *
 */

public class MyRedisSource  implements SourceFunction<HashMap<String, String>> {


    private boolean  isRunning = true;
    private Jedis jedis = null;
    private final Long SLEEP_MILLION = 60000L;

    @Override
    public void run(SourceContext<HashMap<String, String>> sourceContext) throws Exception {

        this.jedis = new Jedis("127.0.0.1", 6379);
        //存储所有国家和大区的对应关系
        HashMap<String, String> keyValueMap = new HashMap<>();

        while (isRunning){

            try {
                keyValueMap.clear();
                Map<String, String> areas = jedis.hgetAll("areas");

                for (Map.Entry<String,String> entry: areas.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");

                    for (String split: splits){
                        // key value 顺序？
                        keyValueMap.put(split, key);
                    }
                }
                if (keyValueMap.size()>0){
                    sourceContext.collect(keyValueMap);
                }

                Thread.sleep(SLEEP_MILLION);
            } catch (JedisConnectionException e){
                this.jedis = new Jedis("127.0.0.1", 6379);
                System.out.println("redis error" + e.getMessage());
            } catch (Exception e) {
                System.out.println(" other error "+ e.getMessage());
            }

        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}