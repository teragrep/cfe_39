package com.teragrep.cfe_39.consumers.kafka;

import org.apache.kafka.clients.consumer.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class KafkaReaderTemp implements AutoCloseable {
    final Logger LOGGER = LoggerFactory.getLogger(KafkaReaderTemp.class);

    private Iterator<ConsumerRecord<byte[], byte[]>> kafkaRecordsIterator = Collections.emptyIterator();

    private final Consumer<byte[], byte[]> kafkaConsumer;

    private final java.util.function.Consumer<List<byte[]>> callbackFunction;

    public KafkaReaderTemp(
            Consumer<byte[], byte[]> kafkaConsumer, java.util.function.Consumer<List<byte[]>> callbackFunction) {
        this.kafkaConsumer = kafkaConsumer;
        this.callbackFunction = callbackFunction;
    }

    public void read() {
        long offset;
        if (!kafkaRecordsIterator.hasNext()) {
            // still need to consume more, infinitely loop because connection problems may cause return of an empty iterator
            ConsumerRecords<byte[], byte[]> kafkaRecords = kafkaConsumer.poll(Duration.ofSeconds(60)); // TODO parametrize
            if (kafkaRecords.isEmpty()) {
                LOGGER.debug("kafkaRecords empty after poll.");
            }
            kafkaRecordsIterator = kafkaRecords.iterator();
        }

        List<byte[]> recordsList = new ArrayList<>();
        while (kafkaRecordsIterator.hasNext()) {
            ConsumerRecord<byte[], byte[]> record = kafkaRecordsIterator.next();
            offset = record.offset(); // offset for HDFS filenames
            LOGGER.debug("adding from offset: " + record.offset()); // TODO: The offsets must be passed outside the class, to the main class that is calling this class.
            recordsList.add(record.value());
        }

        if (!recordsList.isEmpty()) {
            callbackFunction.accept(recordsList);
            kafkaConsumer.commitSync();
            /*
            commitSync():
            It only commits the offsets that were actually polled and processed. If some offsets were not included in the last poll, then those offsets will not be committed.
            It will not commit the latest positions for all subscribed partitions. This would interfere with the Consumer Offset management concept of Kafka to be able to re-start an application where it left off.
            * */
        }
    }

    @Override
    public void close() {
        kafkaConsumer.close(Duration.ofSeconds(60)); // TODO parametrize
    }
}
