package com.flink.wudy.io.async.Request;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.flink.wudy.io.utils.DBUtils;
import com.flink.wudy.sink.kafka.model.CustomInputModel;
import com.flink.wudy.sink.strategy.forward.model.InputModel;
import com.mysql.cj.jdbc.MysqlDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Slf4j
public class AsyncDatabaseRequest extends RichAsyncFunction<CustomInputModel, Tuple2<CustomInputModel, String>> {

    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
        connection = DBUtils.getConnection();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (!connection.isClosed()){
            connection.close();
        }
    }


    @Override
    public void asyncInvoke(CustomInputModel inputModel, ResultFuture<Tuple2<CustomInputModel, String>> resultFuture) throws Exception {
        // 异步I/O客户端发起请求得到Future<String>
        // 创建查询SQL语句
        String sql = "select product_id,product_name from product where product_id = ?";

        try {
            PreparedStatement psmt = connection.prepareStatement(sql);
            psmt.setString(1, inputModel.getProductId());
            ResultSet resultSet = psmt.executeQuery();
            log.info("resultSet={}", resultSet);

            while (resultSet.next()){
                String productName = resultSet.getString("product_name");
                log.info("productName={}", productName);
            }

        } finally {
            if (!connection.isClosed()){
                connection.close();
            }

        }

    }

    @Override
    public void timeout(CustomInputModel input, ResultFuture<Tuple2<CustomInputModel, String>> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
    }
}
