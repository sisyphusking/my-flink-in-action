package com.qq.welink.project.datastream.entity;

import com.google.gson.annotations.SerializedName;

public class ResultEntity {

    @SerializedName("AppId")
    private Long appId;

    @SerializedName("ProjId")
    private Long projectId;

    @SerializedName("ReceivedTimeStamp")
    private Long timeStamp;

    @SerializedName("value")
    private Object value;

    public Long getAppId() {
        return appId;
    }

    public void setAppId(Long appId) {
        this.appId = appId;
    }

    public Long getProjectId() {
        return projectId;
    }

    public void setProjectId(Long projectId) {
        this.projectId = projectId;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

}