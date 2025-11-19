package com.flink.wudy.clickhouse.service;

import com.flink.wudy.clickhouse.model.ClientLog;
import com.flink.wudy.clickhouse.pojo.RuleComplexParam;

/**
 * 原子规则条件匹配接口
 */
public interface BaseRuleService {
    public boolean matchRuleByCondition(ClientLog log, RuleComplexParam ruleComplexParam);
}
