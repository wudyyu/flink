package com.flink.wudy.demo.connect.models;

import com.flink.wudy.demo.flatMap.models.LogModel;

public class OutputModel {

    private String userName;

    private LogModel log;


    public OutputModel(String userName, LogModel log) {
        this.userName = userName;
        this.log = log;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public LogModel getLog() {
        return log;
    }

    public void setLog(LogModel log) {
        this.log = log;
    }

    @Override
    public String toString() {
        return "OutputModel{" +
                "userName='" + userName + '\'' +
                ", log=" + log +
                '}';
    }
}
