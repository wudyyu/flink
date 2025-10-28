package com.flink.wudy.demo.connect.models;

import com.flink.wudy.demo.flatMap.models.LogModel;

import java.util.List;

public class ConnectInputModel2 {

    private String userName;

    private List<LogModel> logs;

    public ConnectInputModel2(String userName, List<LogModel> logs) {
        this.userName = userName;
        this.logs = logs;
    }

    public String getUserName() {
        return userName;
    }

    public List<LogModel> getLogs() {
        return logs;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setLogs(List<LogModel> logs) {
        this.logs = logs;
    }

    @Override
    public String toString() {
        return "ConnectInputModel2{" +
                "userName='" + userName + '\'' +
                ", logs=" + logs +
                '}';
    }
}
