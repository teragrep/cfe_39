package com.teragrep.cfe_39;

import com.teragrep.cfe_39.consumers.kafka.KafkaController;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import com.teragrep.cfe_39.avro.SyslogRecord;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class KafkaConsumerTest {
    // Make sure application.properties has consumer.useMockKafkaConsumer=true enabled for Kafka testing.

    // @Test
    public void configTest() {
        // Configuration tests done, configurations working correctly with the right .jaas and .properties files.
        try {
            Config config = new Config();

            Properties readerKafkaProperties = config.getKafkaConsumerProperties();

            // Test extracting useMockKafkaConsumer value from config.
            boolean useMockKafkaConsumer = Boolean.parseBoolean(
                    readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")
            );
            System.out.println("useMockKafkaConsumer: "+useMockKafkaConsumer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    // @Test
    public void kafkaAndAvroFullTest() throws InterruptedException {
        Config config = null;
        try {
            config = new Config();
        } catch (IOException e){
            System.out.println("Can't load config: " + e);
            System.exit(1);
        } catch (IllegalArgumentException e) {
            System.out.println("Got invalid config: " + e);
            System.exit(1);
        }
        config.setMaximumFileSize(3000); // 10 loops (140 records) are in use at the moment, and that is sized at 36,102 bits.
        KafkaController kafkaController = new KafkaController(config);
        kafkaController.run();
        try {
            int counter = avroReader(1, 2);
            Assertions.assertEquals(140, counter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        cleanup(config, 1, 2);
    }

    // Reads the data from a list of avro files
    public int avroReader(int start, int end) throws IOException {
        // Deserialize Users from disk
        Config config = new Config();
        Path queueDirectory = Paths.get(config.getQueueDirectory());
        int counter = 0;
        int looper = 0;
        int partitionCounter = 0;
        for (int j = 0; j <= 9; j++) {
            for (int i = start; i <= end; i++) {
                File syslogFile = new File(
                        queueDirectory.toAbsolutePath()
                                + File.separator
                                + "testConsumerTopic"
                                + j
                                + "."
                                + i
                );
                DatumReader<SyslogRecord> userDatumReader = new SpecificDatumReader<>(SyslogRecord.class);
                try (DataFileReader<SyslogRecord> dataFileReader = new DataFileReader<>(syslogFile, userDatumReader)) {
                    SyslogRecord user = null;
                    while (dataFileReader.hasNext()) {
                        user = dataFileReader.next(user);
                        System.out.println(syslogFile.getPath());
                        System.out.println(user);
                        counter++;
                        // All the mock data is generated from a set of 14 records.
                        if (looper <= 0) {
                            Assertions.assertEquals("{\"timestamp\": 1650872090804000, \"message\": \"[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 0, \"origin\": \"jla-02.default\"}", user.toString());
                            looper++;
                        } else if (looper == 1) {
                            Assertions.assertEquals("{\"timestamp\": 1650872090806000, \"message\": \"[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 1, \"origin\": \"jla-02.default\"}", user.toString());
                            looper++;
                        } else if (looper == 2) {
                            Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 2, \"origin\": \"jla-02\"}", user.toString());
                            looper++;
                        } else if (looper == 3) {
                            Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 3, \"origin\": \"jla-02\"}", user.toString());
                            looper++;
                        } else if (looper == 4) {
                            Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 4, \"origin\": \"jla-02\"}", user.toString());
                            looper++;
                        } else if (looper == 5) {
                            Assertions.assertEquals("{\"timestamp\": 1650872092238000, \"message\": \"25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 5, \"origin\": \"jla-02.default\"}", user.toString());
                            looper++;
                        } else if (looper == 6) {
                            Assertions.assertEquals("{\"timestamp\": 1650872092239000, \"message\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 6, \"origin\": \"jla-02.default\"}", user.toString());
                            looper++;
                        } else if (looper == 7) {
                            Assertions.assertEquals("{\"timestamp\": 1650872092239000, \"message\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 7, \"origin\": \"jla-02.default\"}", user.toString());
                            looper++;
                        } else if (looper == 8) {
                            Assertions.assertEquals("{\"timestamp\": 1650872092240000, \"message\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 8, \"origin\": \"jla-02.default\"}", user.toString());
                            looper++;
                        } else if (looper == 9) {
                            Assertions.assertEquals("{\"timestamp\": 1650872092240000, \"message\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 9, \"origin\": \"jla-02.default\"}", user.toString());
                            looper++;
                        } else if (looper == 10) {
                            Assertions.assertEquals("{\"timestamp\": 1650872092241000, \"message\": \"25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 10, \"origin\": \"jla-02.default\"}", user.toString());
                            looper++;
                        } else if (looper == 11) {
                            Assertions.assertEquals("{\"timestamp\": 1650872092241000, \"message\": \"25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 11, \"origin\": \"jla-02.default\"}", user.toString());
                            looper++;
                        } else if (looper == 12) {
                            Assertions.assertEquals("{\"timestamp\": 1650872092242000, \"message\": \"25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 12, \"origin\": \"jla-02.default\"}", user.toString());
                            looper++;
                        } else {
                            Assertions.assertEquals("{\"timestamp\": 1650872092243000, \"message\": \"25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 13, \"origin\": \"jla-02.default\"}", user.toString());
                            looper = 0;
                            partitionCounter++;
                        }
                    }
                }
            }
        }
        System.out.println("Total number of records: " + counter);
        return counter;
    }

    // @Test
    public void debugger() {
        try {
            int counter = avroReader(1, 2);
            Assertions.assertEquals(140, counter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Deletes the avro-files that were created during testing.
    public void cleanup(Config config, int start, int end) {
        Path queueDirectory = Paths.get(config.getQueueDirectory());
        for (int j = 0; j <= 9; j++) {
            for (int i = start; i <= end; i++) {
                File syslogFile = new File(
                        queueDirectory.toAbsolutePath()
                                + File.separator
                                + "testConsumerTopic"
                                + j
                                + "."
                                + i
                );
                try {
                    boolean result = Files.deleteIfExists(syslogFile.toPath()); //surround it in try catch block
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
