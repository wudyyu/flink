package com.flink.wudy.window.session.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class OutputModel implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 用户id
     */
    private Integer userId;

    /**
     * 用户在本次会话期间浏览商品总次数
     */
    private Integer count;

    /**
     * 本次会话开始时间戳
     */
    private Long sessionStartTimestamp;
}
