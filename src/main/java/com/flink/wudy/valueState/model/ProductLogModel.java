package com.flink.wudy.valueState.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class ProductLogModel implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long productId;

    private String log;
}
