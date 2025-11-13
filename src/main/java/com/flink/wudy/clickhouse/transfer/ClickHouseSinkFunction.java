package com.flink.wudy.clickhouse.transfer;

import com.alibaba.fastjson.JSON;
import com.flink.wudy.clickhouse.model.BillEntityModel;
import com.flink.wudy.config.ClickHouseConfig;
import com.flink.wudy.convetor.BillConvetor;
import com.flink.wudy.entity.BillEntity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseSinkFunction extends RichSinkFunction<BillEntityModel> {

    private transient Connection connection;
    private transient PreparedStatement preparedStatement;
    private transient List<BillEntity> batchList;
    private static final int BATCH_SIZE = 10;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.connection = ClickHouseConfig.getConnection();
        this.preparedStatement = connection.prepareStatement(
                "INSERT INTO bill (bill_id, project_id, price, region_id, item_id, charge_type, created_at, start_time, end_time)" +
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );
        this.batchList = new ArrayList<>();
    }

    @Override
    public void invoke(BillEntityModel billEntityModel, Context context) throws Exception {
        try {
            // 将数据转换为适合 ClickHouse 的格式并插入
            BillEntity billEntity = BillConvetor.INSTANCE.toBillEntity((billEntityModel));
            batchList.add(billEntity);
            if (batchList.size() >= BATCH_SIZE) {
                flushBatch();
            }
        } catch (Exception e) {
            // 处理异常，例如重试或记录错误日志
            System.out.println("Error inserting data" + e.getLocalizedMessage());
        }
    }

    @Override
    public void close() throws Exception {
        flushBatch();

        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    private void flushBatch() throws SQLException {
        if (batchList.isEmpty()) {
            return;
        }

        for (BillEntity entity : batchList) {
            try {
                preparedStatement.setString(1, entity.getBillId());
                preparedStatement.setLong(2, entity.getProjectId());
                preparedStatement.setBigDecimal(3, entity.getPrice());
                preparedStatement.setString(4, entity.getRegionId());
                preparedStatement.setString(5, entity.getItemId());
                preparedStatement.setString(6, entity.getChargeType());
                preparedStatement.setString(7, entity.getCreatedAt().format(FORMATTER));
                preparedStatement.setLong(8, entity.getStartTime());
                preparedStatement.setLong(9, entity.getEndTime());
                preparedStatement.addBatch();
            } catch (Exception e){
                System.out.println("happen an error=" + e.getLocalizedMessage() + "entity=" + JSON.toJSONString(entity));
            }

        }

        preparedStatement.executeBatch();
        batchList.clear();
    }

}
