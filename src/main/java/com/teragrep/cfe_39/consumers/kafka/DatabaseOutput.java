package com.teragrep.cfe_39.consumers.kafka;

import com.teragrep.cfe_39.Config;
import com.teragrep.cfe_39.metrics.topic.TopicCounter;
import com.teragrep.cfe_39.metrics.RuntimeStatistics;
import com.teragrep.rlo_06.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

// TODO:
//  Alter the class so the output is not SQL (aka. mariadb).
//  Instead the kafka stream should first be deserialized using rlo_06 and then serialized using avro and stored in HDFS.
//  The kafka offsets must be passed to HDFS too.

public class DatabaseOutput implements Consumer<List<byte[]>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseOutput.class);
    private final RFC5424Frame rfc5424Frame = new RFC5424Frame(false);

    private final String table;


    private final RuntimeStatistics runtimeStatistics;
    private final TopicCounter topicCounter;

    private long lastTimeCalled = Instant.now().toEpochMilli();

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    DatabaseOutput(
            Config config,
            String table,
            RuntimeStatistics runtimeStatistics,
            TopicCounter topicCounter
    ) {
        this.table = table;
        this.runtimeStatistics = runtimeStatistics;
        this.topicCounter = topicCounter;
    }

    @Override
    public void accept(List<byte[]> bytes) {
        long thisTime = Instant.now().toEpochMilli();
        long ftook = thisTime - lastTimeCalled;
        topicCounter.setKafkaLatency(ftook);
        LOGGER.debug(ANSI_BLUE + "Fuura searching your batch for <[" + table + "]> with records <" + bytes.size() + "> and took  <" + (ftook) + "> milliseconds. <" + (bytes.size() * 1000L / ftook) + "> EPS. " + ANSI_RESET);
        long batchBytes = 0L;

        for (byte[] byteArray : bytes) {
            batchBytes = batchBytes + byteArray.length;
            InputStream inputStream = new ByteArrayInputStream(byteArray);
            rfc5424Frame.load(inputStream);
            try {
                if(rfc5424Frame.next()) {
                    // TODO: rfc5424Frame has loaded the data, it's ready for deserialization.
                    new RFC5424Timestamp(rfc5424Frame.timestamp).toZonedDateTime().toInstant().getEpochSecond();
                    rfc5424Frame.appName.toString();
                    rfc5424Frame.hostname.toString();
                    rfc5424Frame.msg.toString();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        long start = Instant.now().toEpochMilli();

        // TODO: Add the code for sending the data to HDFS here, performance is measured between the start/end.

        long end = Instant.now().toEpochMilli();

        long took = (end - start);
        topicCounter.setDatabaseLatency(took);

        if (took == 0) {
            took = 1;
        }
        long rps = bytes.size() * 1000L / took;
        topicCounter.setRecordsPerSecond(rps);

        long bps = batchBytes * 1000 / took;
        topicCounter.setBytesPerSecond(bps);

        runtimeStatistics.addAndGetRecords(bytes.size());
        runtimeStatistics.addAndGetBytes(batchBytes);

        topicCounter.addToTotalBytes(batchBytes);
        topicCounter.addToTotalRecords(bytes.size());

        LOGGER.debug(
                ANSI_GREEN
                        + "Sent batch for <[" + table + "]> "
                        + "with records <" + bytes.size() + "> "
                        + "and size <" + batchBytes / 1024 + "> KB "
                        + "took <" + (took) + "> milliseconds. "
                        + "<" + rps + "> RPS. "
                        + "<" + bps / 1024 + "> KB/s "
                        + ANSI_RESET
        );
        lastTimeCalled = Instant.now().toEpochMilli();
    }
}