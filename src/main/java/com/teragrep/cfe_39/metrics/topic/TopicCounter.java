package com.teragrep.cfe_39.metrics.topic;


import java.util.concurrent.atomic.AtomicLong;

public class TopicCounter {
    private final String topicName;
    private final AtomicLong totalRecords = new AtomicLong();
    private final AtomicLong totalBytes = new AtomicLong();
    private final AtomicLong recordsPerSecond = new AtomicLong();
    private final AtomicLong bytesPerSecond = new AtomicLong();
    private final AtomicLong kafkaLatency = new AtomicLong();
    private final AtomicLong databaseLatency = new AtomicLong();

    public TopicCounter(String topicName) {
        this.topicName = topicName;
    }
    public long getTotalRecords() {
        return totalRecords.get();
    }

    public long getTotalBytes () {
        return totalBytes.get();
    }

    public long getRecordsPerSecond() {
        return recordsPerSecond.get();
    }

    public long getBytesPerSecond() {
        return bytesPerSecond.get();
    }

    public String getTopicName() {
        return topicName;
    }

    public long getKafkaLatency() {
        return kafkaLatency.get();
    }

    public long getDatabaseLatency() {
        return databaseLatency.get();
    }

    public void addToTotalRecords(long incrementBy) {
        totalRecords.addAndGet(incrementBy);
    }

    public void addToTotalBytes(long incrementBy) {
        totalBytes.addAndGet(incrementBy);
    }

    public void setRecordsPerSecond(long rps) {
        recordsPerSecond.set(rps);
    }

    public void setBytesPerSecond(long bps) {
        bytesPerSecond.set(bps);
    }

    public void setKafkaLatency(long latency) {
        kafkaLatency.set(latency);
    }

    public void setDatabaseLatency(long latency) {
        databaseLatency.set(latency);
    }
}
