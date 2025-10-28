package com.flink.wudy.window.session.model;

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
     * 用户浏览商品日志生成的时间戳(用户每浏览一件商品就会上报一条日志)
     */
    private Long timestamp;

    public InputModel(Integer userId, Long timestamp) {
        this.userId = userId;
        this.timestamp = timestamp;
    }
}
