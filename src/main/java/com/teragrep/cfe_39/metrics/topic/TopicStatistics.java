package com.teragrep.cfe_39.metrics.topic;

// TODO: Add dropwizard metrics implementation to replace old mxj_01 implementation.

import javax.management.*;
import java.util.List;

public class TopicStatistics {
    private final List<TopicCounter> topicList;

    // FIXME
    // private final DynamicBean dynamicBean;

    public TopicStatistics(List<TopicCounter> topicList) {
        this.topicList = topicList;

        // FIXME
/*        // page
        CompositeDataWriter<TopicCounter> topicCounterWriter = CompositeDataWriter.builder(TopicCounter.class)
                .withTypeName("topic")
                .withTypeDescription("Topic throughput")
                .withSimpleAttribute("topicName", TopicCounter::getTopicName)
                .withSimpleAttribute("totalRecords", TopicCounter::getTotalRecords)
                .withSimpleAttribute("totalBytes", TopicCounter::getTotalBytes)
                .withSimpleAttribute("recordsPerSecond", TopicCounter::getRecordsPerSecond)
                .withSimpleAttribute("bytesPerSecond", TopicCounter::getBytesPerSecond)
                .withSimpleAttribute("kafkaLatency", TopicCounter::getKafkaLatency)
                .withSimpleAttribute("databaseLatency", TopicCounter::getDatabaseLatency)
                .build();

        // book
        TabularDataWriter<TopicCounter> topicListWriter = TabularDataWriter.builder(TopicCounter.class)
                .withTypeName("topics")
                .withTypeDescription("Topics counted")
                .withIndexName("topicName")
                .withCompositeDataWriter(topicCounterWriter)
                .build();


        this.dynamicBean = DynamicBean.builder()
                .withTabularAttribute(
                        "TopicStatistics",
                        () -> topicList,
                        topicListWriter
                )
                .build();*/
    }

    public DynamicMBean register() {
        // FIXME
        /*MXJEndpoint mxjEndpoint = new MXJEndpoint(
                "com.teragrep.cfe_30",
                "Metrics",
                "Topic",
                dynamicBean
        );
        return mxjEndpoint.register();*/
        return null;
    }
}
