package com.flink.wudy.entity;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class BillEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    // 账单id
    private String billId;

    // 渠道id
    private Long projectId;

    // 单价
    private BigDecimal price;

    //数据中心
    private String regionId;

    //计费项id
    private String itemId;

    private String chargeType;

    private LocalDateTime createdAt;

    // 账单开始时间(10分钟账单)
    private Long startTime;

    // 账单结束时间(10分钟账单)
    private Long endTime;
}
