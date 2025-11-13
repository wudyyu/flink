package com.flink.wudy.clickhouse.model;

import lombok.Getter;

@Getter
public enum ChargeType {
    POST_PAID("post_paid", "按量计费"),
    PRE_PAID("pre_paid", "包年包月");

    private final String code;

    private final String name;

    ChargeType(String code, String name) {
        this.code = code;
        this.name = name;
    }

    public static ChargeType getEnumByCode(String code) {
        for (ChargeType enumInfo : ChargeType.values()) {
            if (!code.equals(enumInfo.getCode())) {
                continue;
            }
            return enumInfo;
        }
        throw new RuntimeException("Get EnumByCode Occurs An Error");
    }
}
