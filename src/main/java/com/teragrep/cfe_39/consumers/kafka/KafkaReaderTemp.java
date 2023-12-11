package com.teragrep.cfe_39.consumers.kafka;

import org.apache.kafka.clients.consumer.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class KafkaReaderTemp implements AutoCloseable {
    final Logger LOGGER = LoggerFactory.getLogger(KafkaReaderTemp.class);

    private Iterator<ConsumerRecord<byte[], byte[]>> kafkaRecordsIterator = Collections.emptyIterator();

    private final Consumer<byte[], byte[]> kafkaConsumer;

    private final java.util.function.Consumer<List<RecordOffsetObject>> callbackFunction;

    public KafkaReaderTemp(
            Consumer<byte[], byte[]> kafkaConsumer, java.util.function.Consumer<List<RecordOffsetObject>> callbackFunction) {
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

        List<RecordOffsetObject> recordOffsetObjectList = new ArrayList<>();
        while (kafkaRecordsIterator.hasNext()) {
            ConsumerRecord<byte[], byte[]> record = kafkaRecordsIterator.next();
            LOGGER.debug("adding from offset: " + record.offset());
            // Do filtering here.
            boolean checkStuff = checkIfProcessed(record.topic(), record.partition(), record.offset()); // TODO: Create checkIfProcessed method. Checks if the record has already been processed and stored in HDFS.
            if (!checkStuff) {
                recordOffsetObjectList.add(new RecordOffsetObject(record.topic(), record.partition(), record.offset(), record.value()));
            }else{
                // TODO: The consumer should update its offsets to effectively mark the message as consumed to ensure it is not redelivered, and no further action takes place.
            }
        }

        if (!recordOffsetObjectList.isEmpty()) {
            // This is the DatabaseOutput.accept() function. This is where the idempotent consumer pattern should be implemented.
            // Offset and other required data for HDFS storage are added to the input parameters of the accept() function which processes the consumed record.
            callbackFunction.accept(recordOffsetObjectList);
            kafkaConsumer.commitSync(); // FIXME: Should the commitSync() be moved outside of the if-brackets so the consumer could skip the already processed records properly?

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
