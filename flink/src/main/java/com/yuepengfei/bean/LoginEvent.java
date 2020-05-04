package com.yuepengfei.bean;

import java.io.Serializable;

public class LoginEvent implements Serializable {
    public String userId;
    public String ip;
    public String type_type;

    public LoginEvent(String userId, String ip, String type_type) {
        this.userId = userId;
        this.ip = ip;
        this.type_type = type_type;
    }

    public LoginEvent() {
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getType_type() {
        return type_type;
    }

    public void setType_type(String type_type) {
        this.type_type = type_type;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ip='" + ip + '\'' +
                ", type_type='" + type_type + '\'' +
                '}';
    }
}
