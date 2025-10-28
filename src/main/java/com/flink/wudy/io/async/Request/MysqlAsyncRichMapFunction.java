package com.flink.wudy.io.async.Request;

import com.flink.wudy.io.utils.DBUtils;
import com.flink.wudy.sink.kafka.model.CustomInputModel;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class MysqlAsyncRichMapFunction extends RichAsyncFunction<String, CustomInputModel> {

    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(String line, ResultFuture<CustomInputModel> resultFuture) throws Exception {
        CustomInputModel customInputModel = new CustomInputModel();
        customInputModel.setIncome(11);
        customInputModel.setProductId("car");
        customInputModel.setCount(15);
        customInputModel.setAvgPrice(new BigDecimal("99"));

        Future<String> dbResult =
        DBUtils.executorService.submit(new Callable<String>() {

            @Override
            public String call() throws Exception {
                String productName = null;
                ResultSet resultSet = null;
                PreparedStatement statement = null;
                String sql = "select * from product where product_id = ?";

                try {
                    statement = DBUtils.getConnection().prepareStatement(sql);
                    statement.setString(1,customInputModel.getProductId());

                    resultSet = statement.executeQuery();
                    while (resultSet.next()) {
                        productName= resultSet.getString("product_name");
                    }
                } finally {
                    if (resultSet != null) {
                        resultSet.close();
                    }
                    if (statement != null) {
                        statement.close();
                    }
                }

                return productName;
            }
        });

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return dbResult.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).thenAccept((String productName) -> {
            customInputModel.setProductName(productName);
            resultFuture.complete(Collections.singleton(customInputModel));
        });
    }

    @Override
    public void timeout(String input, ResultFuture<CustomInputModel> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
    }
}
