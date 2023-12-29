package com.teragrep.cfe_39.consumers.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * <h1>Mock Kafka Consumer Factory</h1>
 *
 * Mocked Kafka Consumer factory used for testing.
 *
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public class MockKafkaConsumerFactoryTemp {
    final static private Logger LOGGER = LoggerFactory.getLogger(MockKafkaConsumerFactoryTemp.class);

    private MockKafkaConsumerFactoryTemp() {
    }

    private static void generateEvents(MockConsumer<byte[], byte[]> consumer) {
        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        0L,
                        "2022-04-25T07:34:50.804Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:50.804Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"835bf792-91cf-44e3-976b-518330bb8fd3\" source=\"source\" unixtime=\"1650872090805\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!".getBytes(StandardCharsets.UTF_8)
                )
        );
        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        1L,
                        "2022-04-25T07:34:50.806Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:50.806Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"c3f13f9a-05e2-41bd-b0ad-1eca6fd6fd9a\" source=\"source\" unixtime=\"1650872090806\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!".getBytes(StandardCharsets.UTF_8)
                )
        );
        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        2L,
                        "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"1848d8a1-2f08-4a1e-bec4-ff9e6dd92553\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        3L,
                        "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"5e1a0398-c2a0-468d-a562-c3bb31f0f853\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        4L,
                        "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"6268c3a2-5bda-427f-acce-29416eb445f4\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        5L,
                        "2022-04-25T07:34:52.238Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.238Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"b500dcaf-1101-4000-b6b9-bfb052ddbf86\" source=\"source\" unixtime=\"1650872092238\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        6L,
                        "2022-04-25T07:34:52.239Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.239Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"05363122-51ac-4c0b-a681-f5868081f56d\" source=\"source\" unixtime=\"1650872092239\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        7L,
                        "2022-04-25T07:34:52.239Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.239Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"7bbcd843-b795-4c14-b4a1-95f5d445cbcd\" source=\"source\" unixtime=\"1650872092239\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        8L,
                        "2022-04-25T07:34:52.240Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.240Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"2bc0a9f9-237d-4656-b40a-3038aace37f0\" source=\"source\" unixtime=\"1650872092240\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        9L,
                        "2022-04-25T07:34:52.240Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.240Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"ecf61e8d-e3a7-48ef-9b73-3c5a5243d2e6\" source=\"source\" unixtime=\"1650872092240\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        10L,
                        "2022-04-25T07:34:52.241Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.241Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"bf101d5a-e816-4f51-b132-97f8e3431f8e\" source=\"source\" unixtime=\"1650872092241\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        11L,
                        "2022-04-25T07:34:52.241Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.241Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"ef94d9e9-3c44-4892-b5a6-bf361d13ff97\" source=\"source\" unixtime=\"1650872092241\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        12L,
                        "2022-04-25T07:34:52.242Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.242Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"5bce6e3d-767d-44b4-a044-6c4872f8f2b5\" source=\"source\" unixtime=\"1650872092242\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>("testConsumerTopic",
                        0,
                        13L,
                        "2022-04-25T07:34:52.243Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.243Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"3bb55ce4-0ea7-413a-b403-28b174d7ac99\" source=\"source\" unixtime=\"1650872092243\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );
    }

    public static Consumer<byte[], byte[]> getConsumer() {
        LOGGER.warn("useMockKafkaConsumer is set, using MockKafkaConsumer");
        final MockConsumer<byte[], byte[]> consumer;

        final TopicPartition topicPartition = new TopicPartition("testConsumerTopic", 0);

        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.assign(Collections.singletonList(topicPartition));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L); // start is inclusive
        consumer.updateBeginningOffsets(beginningOffsets);

        generateEvents(consumer);

        HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, 14L); // end is exclusive
        consumer.updateEndOffsets(endOffsets);
        LOGGER.debug(endOffsets.toString());

        // consumer.updatePartitions() function must be used so the consumer.listTopics() function can work and return the list of topics properly.
        List<PartitionInfo> mockPartitionInfo = new ArrayList<>();
        mockPartitionInfo.add(new PartitionInfo("testConsumerTopic", 0, null, null, null));
        consumer.updatePartitions("testConsumerTopic", mockPartitionInfo);

        return consumer;
    }
}