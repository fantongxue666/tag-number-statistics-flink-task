package com.ftx.flink.model;

public class RedisTimeseriesTagMessage {
    public String key;
    public double value;
    public long timestamp;

    public RedisTimeseriesTagMessage() {
    }

    public RedisTimeseriesTagMessage(String key, double value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
