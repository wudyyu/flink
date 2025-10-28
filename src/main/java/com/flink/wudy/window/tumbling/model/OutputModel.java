package com.flink.wudy.window.tumbling.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class OutputModel implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 商品id
     */
    private String productId;

    /**
     * 1分钟内累计商品销售额
     */
    private Integer minuteIncome;

    /**
     * 1分钟内窗口起始Unix时间戳
     */
    private Long minuteStartTimestamp;
}
