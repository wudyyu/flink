package com.flink.wudy.io.utils;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DBUtils {

    public static final String jdbcUrl = "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimeZone=UTC&characterEncoding=utf8";

    public static final String userName = "root";

    public static final String password = "WOaiyuyu123";

    public static final String driverName = "com.mysql.cj.jdbc.Driver";

    public static Connection connection = null;
    
    public static transient ExecutorService executorService = null;

    public static transient DruidDataSource dataSource = null;

    public static int maxPoolConn = 20;

    public static int minPoolSize = 5;

    static {
        executorService = Executors.newFixedThreadPool(maxPoolConn);
        // 新建并打开数据库客户端连接
        dataSource = new DruidDataSource();
        dataSource.setUrl(jdbcUrl);
        dataSource.setDriverClassName(driverName);
        dataSource.setUsername(userName);
        dataSource.setPassword(password);

        // 设置初始连接数
        dataSource.setInitialSize(minPoolSize);
        // 设置最大连接数
        dataSource.setMaxActive(maxPoolConn);
        // 设置最小空闲连接数
        dataSource.setMinIdle(minPoolSize);
    }

    public static Connection getConnection() throws SQLException {
        if (connection == null){
            connection = dataSource.getConnection();
        }
        return connection;
    }
}
