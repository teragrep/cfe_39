/*
   HDFS Data Ingestion for PTH_06 use CFE-39
   Copyright (C) 2022  Fail-Safe IT Solutions Oy

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

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
