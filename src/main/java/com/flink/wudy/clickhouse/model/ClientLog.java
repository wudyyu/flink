package com.flink.wudy.clickhouse.model;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class ClientLog implements Serializable {
    private static final long serialVersionUID = 1L;

    // 用户id
    private String userId;

    // 用户名称
    private String userName;

    // app编号
    private String appId;

    // APP 版本
    private String appVersion;

    // 运营商
    private String carrier;

    // 设备编号
    private String deviceNo;

    // 设备类型
    private String deviceType;

    // 客户端IP
    private String ip;

    // 网络类型 WIFI、4G、5G
    private String netType;

    // 操作系统类型
    private String osName;

    // 操作系统版本
    private String osVersion;

    // 会话id
    private String sessionId;

    // 创建日期
    private String createdTime;

    // 创建详细时间
    private String detailTime;

    // 事件编号
    private String eventId;

    // 事件类型
    private String eventType;

    // 经纬度信息
    private String gps;

    // 事件详细属性
    private Map<String,String> properties;
}
