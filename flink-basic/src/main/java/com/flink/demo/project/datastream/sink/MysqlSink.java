package com.flink.demo.project.datastream.sink;

import com.flink.demo.project.datastream.entity.ResultEntity;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class MysqlSink extends RichSinkFunction <List<ResultEntity>> {

     PreparedStatement ps;
     BasicDataSource dataSource;
     private Connection connection;


    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters)  {
        try {
            super.open(parameters);
            this.dataSource = new BasicDataSource();
            this.connection = getConnection(dataSource);

            String sql = "insert into data_type(appid, projectid, timestamp, value) values(?, ?, ?, ?);";
            ps = this.connection.prepareStatement(sql);
        }catch (Exception e){
            System.out.println("open error: "+e.getMessage());
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<ResultEntity> value, Context context)  {
        try {
        //遍历数据集合
        for (ResultEntity resultEntity : value) {
            ps.setLong(1, resultEntity.getAppId());
            ps.setLong(2, resultEntity.getProjectId());
            ps.setLong(3, resultEntity.getTimeStamp());
            ps.setString(4, resultEntity.getValue().toString());
            ps.addBatch();
        }
            int[] count = ps.executeBatch();//批量后执行
            //System.out.println("insert data: " + count.length + " rows");
        } catch (Exception e){
            System.out.println(" insert error:"+ e.getMessage());
        }

    }


    private static Connection getConnection(BasicDataSource dataSource) throws Exception {

        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("mysql.url");
        dataSource.setUsername("username");
        dataSource.setPassword("mysql.password");
        //设置连接池的一些参数
        dataSource.setInitialSize(5);
        dataSource.setMaxTotal(5);
        dataSource.setMinIdle(5);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("connect to db: " + con);
        } catch (Exception e) {
            System.out.println("mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }


}