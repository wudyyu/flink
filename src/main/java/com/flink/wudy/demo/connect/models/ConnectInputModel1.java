package com.flink.wudy.demo.connect.models;

import com.flink.wudy.demo.flatMap.models.LogModel;


public class ConnectInputModel1 {

    private String userName;

    private LogModel log;

    public ConnectInputModel1(String userName, LogModel log) {
        this.userName = userName;
        this.log = log;
    }

    public String getUserName() {
        return userName;
    }

    public LogModel getLog() {
        return log;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setLog(LogModel log) {
        this.log = log;
    }

    @Override
    public String toString() {
        return "ConnectInputModel1{" +
                "userName='" + userName + '\'' +
                ", log=" + log +
                '}';
    }
}
