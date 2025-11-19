package com.flink.wudy.valueState.function;

import com.flink.wudy.config.MySqlConfig;
import com.flink.wudy.valueState.model.InputModel;
import com.mysql.cj.jdbc.Driver;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

public class MySqlSinkFunction extends RichSinkFunction<InputModel> implements CheckpointedFunction {

    private final int threshold;
    private transient ListState<InputModel> checkPointedState;
    private List<InputModel> bufferedElements;

    private transient Connection connection;
    private transient PreparedStatement preparedStatement;

    public MySqlSinkFunction(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");

        connection = MySqlConfig.getConnection();
        preparedStatement = connection.prepareStatement(MySqlConfig.getBatchInsertSQL());
    }

    @Override
    public void invoke(InputModel value, Context context) throws Exception {
        try {
            bufferedElements.add(value);
            if (bufferedElements.size() >= threshold){
                // 数据批量写入mysql
                for (InputModel inputModel : bufferedElements){
                    preparedStatement.setString(1, inputModel.getProductId());
                    preparedStatement.setLong(2, inputModel.getIncome());
                    preparedStatement.setLong(3, inputModel.getTimestamp());
                    preparedStatement.addBatch();
                }

                preparedStatement.executeBatch();
                bufferedElements.clear();
            }
        } catch (Exception e) {
            // 处理异常，例如重试或记录错误日志
            System.out.println("Error inserting data" + e.getLocalizedMessage());
        }
    }

    @Override
    public void close() throws Exception {
        // 关闭时强制刷写剩余数据
        if (!bufferedElements.isEmpty()) {
            for (InputModel inputModel : bufferedElements) {
                preparedStatement.setString(1, inputModel.getProductId());
                preparedStatement.setLong(2, inputModel.getIncome());
                preparedStatement.setLong(3, inputModel.getTimestamp());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            bufferedElements.clear();
        }

        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        for (InputModel element : bufferedElements) {
            checkPointedState.add(element);
        }

        checkPointedState.clear();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<InputModel> descriptor = new ListStateDescriptor<InputModel>("buffered-elements", InputModel.class);
        checkPointedState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()){
            for (InputModel element : checkPointedState.get()){
                bufferedElements.add(element);
            }
        }
    }
}
