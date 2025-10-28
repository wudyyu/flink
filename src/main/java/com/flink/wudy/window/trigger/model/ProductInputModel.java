package com.flink.wudy.window.trigger.model;

import lombok.Builder;
import lombok.Data;
import java.io.Serializable;

@Data
@Builder
public class ProductInputModel implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 商品id
     */
    private String productId;

    /**
     * 商品销售额
     */
    private Long income;

    /**
     * 商品售出时的Unix时间戳
     */
    private Long timestamp;

    public ProductInputModel(String productId, Long income, Long timestamp) {
        this.productId = productId;
        this.income = income;
        this.timestamp = timestamp;
    }
}
