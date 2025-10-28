package com.flink.wudy.window.sliding.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class InputModel implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 用户id
     */
    private Integer userId;

    /**
     * 上报用户心跳日志的时间戳
     */
    private Long timestamp;

    public InputModel(Integer userId, Long timestamp) {
        this.userId = userId;
        this.timestamp = timestamp;
    }
}
