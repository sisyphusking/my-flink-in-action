package com.flink.demo.project.datastream.sink;

import com.flink.demo.project.datastream.util.JDBCUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SinkMysqlUtil extends  RichSinkFunction<Tuple6<Long, Long, Long, Long,Long,String>>{

    private JDBCUtil mysqlUtil;
    private static final Logger log = LoggerFactory.getLogger(SinkMysqlUtil.class);

    @Override
    public void open(Configuration parameters)  {
        try {
            super.open(parameters);

            if (mysqlUtil==null) {
                mysqlUtil = new JDBCUtil("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/data_space",
                        "root", "root");
            }

        }catch (Exception e){
            log.error("open error: "+e.getMessage());
        }

    }

    @Override
    public void close() throws Exception {

        super.close();
        if (mysqlUtil != null) {
            mysqlUtil.closeAll();
        }
    }


    @Override
    public void invoke(Tuple6<Long, Long, Long, Long,Long,String> value, Context context)  {
        try {

            String sql = "insert into alert(invadeNum, fireNum, crowdNum, retensionNum,total, time) values(?, ?, ?, ?, ?, ?)";
            Object[] param = {value.f0, value.f1,value.f2,value.f3,value.f4,value.f5};
            int rowNum = this.mysqlUtil.executeUpdate(sql, param);

        } catch (Exception e){
            log.error(" insert error:"+ e.getMessage());
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof SinkMysqlUtil)) return false;

        SinkMysqlUtil that = (SinkMysqlUtil) o;

        return new EqualsBuilder()
                .append(mysqlUtil, that.mysqlUtil)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(mysqlUtil)
                .toHashCode();
    }
}