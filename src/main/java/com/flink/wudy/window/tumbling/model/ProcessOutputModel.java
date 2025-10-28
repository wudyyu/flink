package com.flink.wudy.window.tumbling.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class ProcessOutputModel implements Serializable {
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
     * 指标类型
     */
    private String type;

    /**
     * 访问次数
     */
    private Integer pv;
}
