package com.flink.wudy.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ClickHouseConfig {
    private static final String URL = "jdbc:clickhouse://localhost:8123/wudy";

    static {
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClickHouse驱动加载失败", e);
        }
    }

    public static Connection getConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "default");
        properties.setProperty("password", "WOaiyuyu123");
        return DriverManager.getConnection(URL, properties);
    }

}
