package com.demo.flinkiceberg.warshouse.bean;

import java.util.List;

public class ClickEvent {

    private int webpageId;

    private String uid;

    private String productId;

    private String cookieId;

    private int expendTime;

    private long updateTime;

    public int getWebpageId() {
        return webpageId;
    }

    public void setWebpageId(int webpageId) {
        this.webpageId = webpageId;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getCookieId() {
        return cookieId;
    }

    public void setCookieId(String cookieId) {
        this.cookieId = cookieId;
    }

    public int getExpendTime() {
        return expendTime;
    }

    public void setExpendTime(int expendTime) {
        this.expendTime = expendTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "ClickEvent{" +
                "webpageId=" + webpageId +
                ", uid='" + uid + '\'' +
                ", productId='" + productId + '\'' +
                ", cookieId='" + cookieId + '\'' +
                ", expendTime=" + expendTime +
                ", updateTime=" + updateTime +
                '}';
    }
}
