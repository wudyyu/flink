package com.flink.wudy.demo.flatMap.models;

import java.util.Objects;

public class FlatMapOutputModel {

    public String userName;

    public LogModel log;

    public FlatMapOutputModel(String userName, LogModel log) {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlatMapOutputModel)) return false;
        FlatMapOutputModel that = (FlatMapOutputModel) o;
        return Objects.equals(userName, that.userName) && Objects.equals(log, that.log);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName, log);
    }
}
