package com.flink.wudy.valueState.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class ConfigModel {
    private List<Long> productIds;
}
