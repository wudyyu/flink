package com.flink.wudy.sink.strategy.forward.model;


import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class InputModel implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer indexOfSourceSubTask;

    public InputModel() {}

    public InputModel(Integer indexOfSourceSubTask) {
        this.indexOfSourceSubTask = indexOfSourceSubTask;
    }
}
