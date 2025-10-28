package com.flink.wudy.demo.flatMap.models;


import java.util.Objects;

public class LogModel{

    public String page;

    public String button;

    public LogModel(String page, String button) {
        this.page = page;
        this.button = button;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public String getButton() {
        return button;
    }

    public void setButton(String button) {
        this.button = button;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogModel)) return false;
        LogModel logModel = (LogModel) o;
        return Objects.equals(page, logModel.page) && Objects.equals(button, logModel.button);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page, button);
    }

    @Override
    public String toString() {
        return "LogModel{" +
                "page='" + page + '\'' +
                ", button='" + button + '\'' +
                '}';
    }
}
