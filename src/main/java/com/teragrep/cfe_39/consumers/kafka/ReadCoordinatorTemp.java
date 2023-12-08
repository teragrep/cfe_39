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
    private final Consumer<List<byte[]>> callbackFunction;
    private boolean run = true;
    private long offset;

    public ReadCoordinatorTemp(
            String queueTopic,
            Properties readerKafkaProperties,
            Consumer<List<byte[]>> callbackFunction)
    {
        this.queueTopic = queueTopic;
        this.readerKafkaProperties = readerKafkaProperties;
        this.callbackFunction = callbackFunction;
    }

    private KafkaReaderTemp createKafkaReader(Properties readerKafkaProperties,
                                          String topic,
                                          Consumer<List<byte[]>> callbackFunction,
                                          boolean useMockKafkaConsumer) {

        org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> kafkaConsumer;
        if (useMockKafkaConsumer) {
            kafkaConsumer = MockKafkaConsumerFactoryTemp.getConsumer();
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