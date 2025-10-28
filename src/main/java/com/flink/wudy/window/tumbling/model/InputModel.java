package com.flink.wudy.window.tumbling.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class InputModel implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 商品id
     */
    private String productId;

    /**
     * 商品销售额
     */
    private Integer income;

    /**
     * 商品售出时的Unix时间戳
     */
    private Long timestamp;

    public InputModel(String productId, Integer income, Long timestamp) {
        this.productId = productId;
        this.income = income;
        this.timestamp = timestamp;
    }
}
