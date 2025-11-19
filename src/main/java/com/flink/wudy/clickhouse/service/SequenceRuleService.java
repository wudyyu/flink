package com.flink.wudy.clickhouse.service;

import com.flink.wudy.clickhouse.pojo.RuleComplexParam;
import org.apache.flink.api.common.state.ListState;
import com.flink.wudy.clickhouse.model.ClientLog;

public interface SequenceRuleService {

    boolean matchRuleSequenceCount(ListState<ClientLog> clientLogs, RuleComplexParam ruleComplexParam);

}
