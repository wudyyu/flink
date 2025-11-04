package com.flink.wudy.io.async.model;

import lombok.Data;

@Data
public class UserClickMsg {
    private String platform;

    private Long userId;

    //点击类型：1:收藏，2:取消收藏，3:查看，4:屏蔽
    private int action;

    //商品id
    private Long goodId;
}
