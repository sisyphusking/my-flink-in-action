package com.flink.demo.project.datastream.util;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JDBCUtil {

    //这里注意，一定不要使用静态变量
    private  String driver = null;
    private  String url = null;
    private  String username = null;
    private  String password = null;

    private CallableStatement callableStatement = null;
    private Connection conn = null;
    private PreparedStatement pst = null;
    private ResultSet rst = null;

    private BasicDataSource dataSource = null;

    private static final Logger log = LoggerFactory.getLogger(JDBCUtil.class);

    public JDBCUtil(String driver, String url, String username, String password) {

        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    /**
     * 建立数据库连接
     *
     * @return 数据库连接
     */
    public Connection getConnection() {
        try {


            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);

//            dataSource = new BasicDataSource();
//            dataSource.setDriverClassName(driver);
//            dataSource.setUrl(url);
//            dataSource.setUsername(username);
//            dataSource.setPassword(password);
//            //默认设置连接池的一些参数
//            dataSource.setInitialSize(2);
//            dataSource.setMaxTotal(2);
//            dataSource.setMinIdle(2);
//
//            conn = dataSource.getConnection();

        } catch (Exception e) {
            log.error(" get connection error: " + e.getMessage());
        }
        return conn;
    }

    /**
     * insert update delete SQL语句的执行的统一方法
     *
     * @param sql    SQL语句
     * @param params 参数数组，若没有参数则为null
     * @return 受影响的行数
     */
    public int executeUpdate(String sql, Object[] params) {
        // 受影响的行数
        int affectedLine = 0;

        try {
            // 获得连接
            conn = this.getConnection();
            // 调用SQL
            pst = conn.prepareStatement(sql);

            // 参数赋值
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pst.setObject(i + 1, params[i]);
                }
            }

            affectedLine = pst.executeUpdate();

        } catch (SQLException e) {
            log.error(" executeUpdate: " + e.getMessage());
        }

        return affectedLine;
    }

    /**
     * SQL 查询将查询结果直接放入ResultSet中
     *
     * @param sql    SQL语句
     * @param params 参数数组，若没有参数则为null
     * @return 结果集
     */
    private ResultSet executeQueryRS(String sql, Object[] params) {
        try {
            // 获得连接
            conn = this.getConnection();

            // 调用SQL
            pst = conn.prepareStatement(sql);

            // 参数赋值
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pst.setObject(i + 1, params[i]);
                }
            }

            // 执行
            rst = pst.executeQuery(sql);

        } catch (SQLException e) {
            log.error(" executeQueryRS error: " + e.getMessage());
        }

        return rst;
    }

    /**
     * SQL 查询将查询结果：一行一列
     *
     * @param sql    SQL语句
     * @param params 参数数组，若没有参数则为null
     * @return 结果集
     */
    public Object executeQuerySingle(String sql, Object[] params) {
        Object object = null;
        try {
            // 获得连接
            conn = this.getConnection();

            // 调用SQL
            pst = conn.prepareStatement(sql);

            // 参数赋值
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pst.setObject(i + 1, params[i]);
                }
            }

            // 执行
            rst = pst.executeQuery();

        } catch (SQLException e) {
            log.error(" executeQuerySingle error: " + e.getMessage());
        }
        return rst;
    }

    /**
     * 获取结果集，并将结果放在List中
     *
     * @param sql SQL语句
     *            params  参数，没有则为null
     * @return List
     * 结果集
     */
    public List<Object> excuteQuery(String sql, Object[] params) {
        // 执行SQL获得结果集
        ResultSet rs = executeQueryRS(sql, params);

        // 创建ResultSetMetaData对象
        ResultSetMetaData rsmd = null;

        // 结果集列数
        int columnCount = 0;
        try {
            rsmd = rs.getMetaData();

            // 获得结果集列数
            columnCount = rsmd.getColumnCount();
        } catch (SQLException e) {
            log.error(" get data : " + e.getMessage());
        }

        // 创建List
        List<Object> list = new ArrayList<Object>();

        try {
            // 将ResultSet的结果保存到List中
            while (rs.next()) {
                Map<String, Object> map = new HashMap<String, Object>();
                for (int i = 1; i <= columnCount; i++) {
                    map.put(rsmd.getColumnLabel(i), rs.getObject(i));
                }
                list.add(map);//每一个map代表一条记录，把所有记录存在list中
            }
        } catch (SQLException e) {
            log.error(" excuteQuery error: " + e.getMessage());
        }

        return list;
    }

    /**
     * 存储过程带有一个输出参数的方法
     *
     * @param sql         存储过程语句
     * @param params      参数数组
     * @param outParamPos 输出参数位置
     * @param SqlType     输出参数类型
     * @return 输出参数的值
     */
    public Object excuteQuery(String sql, Object[] params, int outParamPos, int SqlType) {


        Object object = null;
        conn = this.getConnection();
        try {
            // 调用存储过程
            // prepareCall:创建一个 CallableStatement 对象来调用数据库存储过程。
            callableStatement = conn.prepareCall(sql);

            // 给参数赋值
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    callableStatement.setObject(i + 1, params[i]);
                }
            }

            // 注册输出参数
            callableStatement.registerOutParameter(outParamPos, SqlType);

            // 执行
            callableStatement.execute();

            // 得到输出参数
            object = callableStatement.getObject(outParamPos);

        } catch (SQLException e) {
            log.error(" excuteQuery1 error: " + e.getMessage());
        }

        return object;
    }

    /**
     * 关闭所有资源
     */
    public void closeAll() {
        // 关闭结果集对象
        if (rst != null) {
            try {
                rst.close();
            } catch (SQLException e) {
                log.error(" rst: " + e.getMessage());
            }
        }

        // 关闭PreparedStatement对象
        if (pst != null) {
            try {
                pst.close();
            } catch (SQLException e) {
                log.error(" pst: " + e.getMessage());
            }
        }

        // 关闭CallableStatement 对象
        if (callableStatement != null) {
            try {
                callableStatement.close();
            } catch (SQLException e) {
                log.error(" callableStatement : " + e.getMessage());
            }
        }

        // 关闭Connection 对象
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error("conn: " + e.getMessage());
            }
        }

        if (dataSource != null) {
            try {
                dataSource.close();
            } catch (SQLException e) {
                log.error(" dataSource: " + e.getMessage());
            }
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof JDBCUtil)) return false;

        JDBCUtil jdbcUtil = (JDBCUtil) o;

        return new EqualsBuilder()
                .append(callableStatement, jdbcUtil.callableStatement)
                .append(conn, jdbcUtil.conn)
                .append(pst, jdbcUtil.pst)
                .append(rst, jdbcUtil.rst)
                .append(dataSource, jdbcUtil.dataSource)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(callableStatement)
                .append(conn)
                .append(pst)
                .append(rst)
                .append(dataSource)
                .toHashCode();
    }


    public static void main(String[] args) {

        JDBCUtil jdbcUtil = new JDBCUtil("org.postgresql.Driver", "jdbc:postgresql://10.12.90.193:5432/mvsspace?currentSchema=public",
                "postgres", "postgres");
        String sql = "select  * from  geo_object limit 1";
        System.out.println(jdbcUtil.executeQuerySingle(sql, null));


        System.out.println("-----------");
        JDBCUtil jdbcUtil1 = new JDBCUtil("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/data_space",
                "root", "mysql1012!");
        Object[] objects1 = {1, 3, 3, 2, 3, "strstr22"};
        String sql1 = "insert into alert(invadeNum, fireNum, crowdNum, retensionNum,total, time) values(?, ?, ?, ?, ?, ?)";
        System.out.println(jdbcUtil.executeUpdate(sql1, objects1));
        jdbcUtil1.closeAll();
        jdbcUtil.closeAll();
    }


}