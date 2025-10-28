package com.flink.wudy.io.async.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class User implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer id;

    private Integer userNum;

    private String userName;

    private Integer age;

    public User(Integer id, Integer userNum, String userName, Integer age) {
        this.id = id;
        this.userNum = userNum;
        this.userName = userName;
        this.age = age;
    }

}
