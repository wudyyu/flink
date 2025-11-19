package com.flink.wudy.clickhouse.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * 封装 组合+顺序 组合的复杂规则
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleComplexParam implements Serializable {

    // 规则编号
    public String ruleId;

    // 规则名称
    public String ruleName;

    // 基本规则
    private HashMap<String, String> baseRuleParams;

    // 组合规则
    private List<RuleEventExecuteParam> combinationRuleParams;

    // 顺序规则
    private List<RuleEventExecuteParam> sequenceRuleParams;


}
