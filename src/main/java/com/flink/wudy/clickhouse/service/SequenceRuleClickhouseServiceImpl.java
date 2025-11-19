package com.flink.wudy.clickhouse.service;

import com.flink.wudy.clickhouse.model.ClientLog;
import com.flink.wudy.clickhouse.pojo.RuleComplexParam;
import com.flink.wudy.config.ClickHouseConfig;
import org.apache.flink.api.common.state.ListState;

import java.sql.Connection;
import java.sql.SQLException;

public class SequenceRuleClickhouseServiceImpl implements SequenceRuleService{
    public static Connection connection;

    static {
        try {
            connection = ClickHouseConfig.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean matchRuleSequenceCount(ListState<ClientLog> clientLogs, RuleComplexParam ruleComplexParam) {
        return false;
    }
}
