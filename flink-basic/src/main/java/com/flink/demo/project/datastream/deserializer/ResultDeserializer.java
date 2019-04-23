package com.flink.demo.project.datastream.deserializer;

import com.google.gson.Gson;
import com.flink.demo.project.datastream.entity.ResultEntity;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

public class ResultDeserializer implements KeyedDeserializationSchema<ResultEntity> {

    private Gson gson ;

    //这里要注意取第二个bytes
    @Override
    public ResultEntity deserialize(byte[] bytes, byte[] bytes1, String s, int i, long l) throws IOException {

        //这里有个坑，一定不要在上面初始化；不然会报
        if (gson ==null){
            gson = new Gson();
        }

        ResultEntity  resultEntity = gson.fromJson(new String(bytes1) , ResultEntity.class );
        return resultEntity;
    }
    @Override
    public boolean isEndOfStream(ResultEntity resultEntity) {
        return false;
    }

    @Override
    public TypeInformation<ResultEntity> getProducedType() {
        return  getForClass(ResultEntity.class);
    }
}


//public class ResultDeserializer  implements DeserializationSchema<ResultEntity> , SerializationSchema<ResultEntity> {
//
//    private Gson gson = new Gson();
//
//    @Override
//    public ResultEntity deserialize(byte[] bytes) throws IOException {
//
//        ResultEntity resultEntity = gson.fromJson(new String(bytes), ResultEntity.class);
//        return  resultEntity;
//    }
//
//    @Override
//    public boolean isEndOfStream(ResultEntity resultEntity) {
//        return false;
//    }
//
//    @Override
//    public TypeInformation<ResultEntity> getProducedType() {
//        return  getForClass(ResultEntity.class);
//    }
//
//    @Override
//    public byte[] serialize(ResultEntity resultEntity) {
//        return resultEntity.toString().getBytes();
//    }
//}