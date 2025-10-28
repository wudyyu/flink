package com.flink.wudy.window.tumbling.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class ReduceOutputModel implements Serializable {
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


}
