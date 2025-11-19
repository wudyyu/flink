package com.flink.wudy.config;

import com.mysql.jdbc.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class MySqlConfig {
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/bss?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true";

    private static final String USERNAME = "root";
    private static final String PASSWORD = "";  // 修正拼写错误

    private static transient Connection connection;

    private transient PreparedStatement preparedStatement;

    private static final String INSERT_SQL = "INSERT INTO product_sales (product_id, income, sale_time) VALUES (?, ?, ?)";

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("ClickHouse驱动加载失败", e);
        }
    }

    public static Connection getConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", USERNAME);
        properties.setProperty("password", PASSWORD);
        // 添加连接超时和验证设置
        properties.setProperty("connectTimeout", "5000");
        properties.setProperty("socketTimeout", "30000");

        return DriverManager.getConnection(JDBC_URL, properties);
    }

    public static String getBatchInsertSQL(){
        return INSERT_SQL;
    }

}
