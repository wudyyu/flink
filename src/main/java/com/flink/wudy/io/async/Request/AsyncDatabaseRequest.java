package com.flink.wudy.io.async.Request;

import com.flink.wudy.io.utils.DBUtils;
import com.flink.wudy.sink.kafka.model.CustomInputModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);

        PreparedStatement psmt = connection.prepareStatement(sql);
        psmt.setString(1, inputModel.getProductId());
        CompletableFuture.supplyAsync(() -> {
            try {
                return psmt.executeQuery();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }, fixedThreadPool).thenAccept((ResultSet resultSet) -> {
            String productId = "";
            String productName = "";
            try {
                if (resultSet != null && resultSet.next()){
                    productId = resultSet.getString("product_id");
                    productName = resultSet.getString("product_name");
                    System.out.println("productId=" + productId + "," +"productName=" + productName);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            resultFuture.complete(Collections.singleton(new Tuple2<>(inputModel, productId)));
        });

    }

    @Override
    public void timeout(CustomInputModel input, ResultFuture<Tuple2<CustomInputModel, String>> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
        resultFuture.complete(Collections.singleton(new Tuple2<>(input, "UNKNOW")));
    }
}
