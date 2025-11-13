package com.flink.wudy.convetor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ClickHouseDateTimeConverter {
    private static final DateTimeFormatter CLICKHOUSE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将String类型的时间转换为ClickHouse兼容的DateTime格式
     */
    public static String convertToClickHouseDateTime(String dateTimeStr) {
        try {
            // 尝试解析为LocalDateTime并格式化为ClickHouse接受的格式
            LocalDateTime dateTime = LocalDateTime.parse(dateTimeStr,
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return dateTime.format(CLICKHOUSE_FORMATTER);
        } catch (Exception e) {
            // 如果已经是正确格式，直接返回
            return dateTimeStr;
        }
    }

    /**
     * 验证时间字符串是否符合ClickHouse DateTime格式
     */
    public static boolean isValidClickHouseDateTime(String dateTimeStr) {
        try {
            LocalDateTime.parse(dateTimeStr, CLICKHOUSE_FORMATTER);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
