package com.flink.wudy.valueState.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class UserProductModel implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 商品id
     */
    private String productId;

    /**
     * 用户id
     */
    private String userId;

    /**
     * 商品售出时的Unix时间戳
     */
    private Long timestamp;

}
