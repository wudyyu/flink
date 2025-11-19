package com.flink.wudy.clickhouse.service;

import com.flink.wudy.clickhouse.model.ClientLog;
import com.flink.wudy.clickhouse.pojo.RuleComplexParam;

import java.util.HashMap;
import java.util.Set;

public class BaseRuleServiceImpl implements BaseRuleService{

    static {

    }

    /**
     * 查询规则中的条件和事件的属性是否匹配
     * @param log
     * @param ruleComplexParam
     * @return
     */
    @Override
    public boolean matchRuleByCondition(ClientLog log, RuleComplexParam ruleComplexParam) {
        // 1.从规则中取出条件
        HashMap<String, String> baseRuleParams = ruleComplexParam.getBaseRuleParams();

        // 2.匹配和clientLog的条件是否相等
        Set<String> conditionNames = baseRuleParams.keySet();


        return false;
    }
}
