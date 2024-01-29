package com.teragrep.cfe_39.consumers.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

public class ReadCoordinator implements Runnable {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReadCoordinator.class);

    private final String queueTopic;
    private final Properties readerKafkaProperties;
    private final Consumer<List<RecordOffsetObject>> callbackFunction;
    private boolean run = true;

    public ReadCoordinator(
            String queueTopic,
            Properties readerKafkaProperties,
            Consumer<List<RecordOffsetObject>> callbackFunction)
    {
        this.queueTopic = queueTopic;
        this.readerKafkaProperties = readerKafkaProperties;
        this.callbackFunction = callbackFunction;
    }

    private KafkaReader createKafkaReader(Properties readerKafkaProperties,
                                          String topic,
                                          Consumer<List<RecordOffsetObject>> callbackFunction,
                                          boolean useMockKafkaConsumer) {

        org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> kafkaConsumer;
        if (useMockKafkaConsumer) { // Test mode is on, create mock consumers with assigned partitions that are not overlapping with each other.
            String name = Thread.currentThread().getName(); // Use thread name to identify which thread is running the code.
            if (Objects.equals(name, "testConsumerTopic1")) {
                kafkaConsumer = MockKafkaConsumerFactoryTemp.getConsumer(1); // creates a Kafka MockConsumer that has the odd numbered partitions assigned to it.
            }else {
                kafkaConsumer = MockKafkaConsumerFactoryTemp.getConsumer(2); // creates a Kafka MockConsumer that has the even numbered partitions assigned to it.
            }
        } else { // Test mode is off, subscribe method should handle assigning the partitions automatically to the consumer based on group id parameters of readerKafkaProperties.
            kafkaConsumer = new KafkaConsumer<>(readerKafkaProperties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            kafkaConsumer.subscribe(Collections.singletonList(topic));
        }

        return new KafkaReader(kafkaConsumer, callbackFunction);
    }

    // Part or Runnable implementation, called when the thread is started.
    @Override
    public void run() {
        boolean useMockKafkaConsumer = Boolean.parseBoolean(readerKafkaProperties.getProperty("useMockKafkaConsumer", "false"));
        try (
                KafkaReader kafkaReader = createKafkaReader(
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

    // remove?
    public void stop() {
        run = false;
    }
}