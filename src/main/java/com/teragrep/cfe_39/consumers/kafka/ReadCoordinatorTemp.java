package com.teragrep.cfe_39.consumers.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

public class ReadCoordinatorTemp implements Runnable {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReadCoordinatorTemp.class);

    private final String queueTopic;
    private final Properties readerKafkaProperties;
    private final Consumer<List<RecordOffsetObject>> callbackFunction;
    private boolean run = true;
    private long offset;

    public ReadCoordinatorTemp(
            String queueTopic,
            Properties readerKafkaProperties,
            Consumer<List<RecordOffsetObject>> callbackFunction)
    {
        this.queueTopic = queueTopic;
        this.readerKafkaProperties = readerKafkaProperties;
        this.callbackFunction = callbackFunction;
    }

    private KafkaReaderTemp createKafkaReader(Properties readerKafkaProperties,
                                          String topic,
                                          Consumer<List<RecordOffsetObject>> callbackFunction,
                                          boolean useMockKafkaConsumer) {

        org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> kafkaConsumer;
        if (useMockKafkaConsumer) {
            kafkaConsumer = MockKafkaConsumerFactoryTemp.getConsumer(); // FIXME: PARTITIONS ARE IN WRONG ORDER IN kafkaConsumer, 7856341209. Should be 0123456789 or in reverse. This is clearly the point of origin for the issue as the AVRO-file contents are in identical (wrong) order.
        } else {
            kafkaConsumer = new KafkaConsumer<>(readerKafkaProperties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            kafkaConsumer.subscribe(Collections.singletonList(topic));
        }

        return new KafkaReaderTemp(kafkaConsumer, callbackFunction);
    }

    @Override
    public void run() {
        boolean useMockKafkaConsumer = Boolean.parseBoolean(
                readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")
        );


        try (
                KafkaReaderTemp kafkaReader = createKafkaReader(
                        readerKafkaProperties,
                        queueTopic,
                        callbackFunction,
                        useMockKafkaConsumer
                )
        ) {
            while (run) {
                kafkaReader.read();
            }
        }
    }

    public void stop() {
        run = false;
    }
}