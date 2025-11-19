package com.flink.wudy.clickhouse.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;

/**
 * 把规则绑定到该类的实例
 * 规则分类：普通规则、组合规则、顺序规则
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleEventExecuteParam implements Serializable {

    // 事件类型
    private String eventId;

    // 规则属性
    private HashMap<String, String> properties;

    // 规则事件的开始/结束时间
    private Long eventStartTime;
    private Long eventEndTime;

    // 阈值
    private int threshold;

    // 组合规则满足的次数
    private int combinationMatchCount;


}
