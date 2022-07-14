package com.ftx.flink.model;

import java.util.Date;

public class DataLakeTagMessage {
    public Date dt;
    public Date ts;
    public String tagNo;
    public String phdTag;
    public String deviceId;
    public Long dateTime;
    public Double tagValue;
    public Integer confidence;
    public Long formattedTime;
    public String dataType;

    public DataLakeTagMessage(Date dt, Date ts, String tagNo, String phdTag, String deviceId, Long dateTime, Double tagValue, Integer confidence, Long formattedTime, String dataType) {
        this.dt = dt;
        this.ts = ts;
        this.tagNo = tagNo;
        this.phdTag = phdTag;
        this.deviceId = deviceId;
        this.dateTime = dateTime;
        this.tagValue = tagValue;
        this.confidence = confidence;
        this.formattedTime = formattedTime;
        this.dataType = dataType;
    }

    public DataLakeTagMessage() {
    }

    public Date getDt() {
        return dt;
    }

    public void setDt(Date dt) {
        this.dt = dt;
    }

    public Date getTs() {
        return ts;
    }

    public void setTs(Date ts) {
        this.ts = ts;
    }

    public String getTagNo() {
        return tagNo;
    }

    public void setTagNo(String tagNo) {
        this.tagNo = tagNo;
    }

    public String getPhdTag() {
        return phdTag;
    }

    public void setPhdTag(String phdTag) {
        this.phdTag = phdTag;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getDateTime() {
        return dateTime;
    }

    public void setDateTime(Long dateTime) {
        this.dateTime = dateTime;
    }

    public Double getTagValue() {
        return tagValue;
    }

    public void setTagValue(Double tagValue) {
        this.tagValue = tagValue;
    }

    public Integer getConfidence() {
        return confidence;
    }

    public void setConfidence(Integer confidence) {
        this.confidence = confidence;
    }

    public Long getFormattedTime() {
        return formattedTime;
    }

    public void setFormattedTime(Long formattedTime) {
        this.formattedTime = formattedTime;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
}
