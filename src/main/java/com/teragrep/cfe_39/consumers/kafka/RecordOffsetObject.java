package com.teragrep.cfe_39.consumers.kafka;

// This is the class for handling the Kafka record topic/partition/offset data that are required for HDFS storage.
public class RecordOffsetObject {
    public String topic;
    public Integer partition;
    public Long offset;
    public byte[] record;

    public RecordOffsetObject(
            String topic,
            int partition,
            long offset,
            byte[] record
    ) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.record = record;
    }
}
