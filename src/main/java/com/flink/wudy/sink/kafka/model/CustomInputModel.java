package com.flink.wudy.sink.kafka.model;

import com.flink.wudy.sink.strategy.forward.model.InputModel;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
public class CustomInputModel extends InputModel implements Serializable{

    private static final long serialVersionUID = 1L;

    /**
     * 商品id
     */
    private String productId;

    /**
     * 商品名称
     */
    private String productName;


    /**
     * 商品销售额
     */
    private Integer income;

    /**
     * 商品销量
     */
    private Integer count;

    /**
     * 商品平均销售额
     */
    private BigDecimal avgPrice;

}
