package com.flink.wudy.window.sliding.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class OutputModel implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 在线用户数
     */
    private Integer uv;

    /**
     * 窗口的开始时间戳
     */
    private Long minuteStartTimestamp;
}
