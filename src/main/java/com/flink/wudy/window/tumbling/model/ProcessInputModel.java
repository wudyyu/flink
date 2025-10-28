package com.flink.wudy.window.tumbling.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class ProcessInputModel implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 用户id
     */
    private Integer userId;

    /**
     * 商品id
     */
    private String productId;

    /**
     * 用户访问商品的时间戳
     */
    private Long timestamp;

    public ProcessInputModel(Integer userId, String productId, Long timestamp) {
        this.userId = userId;
        this.productId = productId;
        this.timestamp = timestamp;
    }
}
