package com.flink.wudy.window.tumbling.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class AggregateOutputModel implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 商品id
     */
    private String productId;

    /**
     * 窗口开始时间戳
     */
    private String windowStart;

    /**
     * 1min内商品平均销售额
     */
    private Long avgIncome;

    /**
     * 1min内商品的总销售量
     */
    private Long allCnt;

    /**
     * 1min内商品的总销售额
     */
    private Long allIncome;

    private Long timeStamp;
}
