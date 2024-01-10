package com.teragrep.cfe_39;

import com.teragrep.cfe_39.consumers.kafka.KafkaController;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import com.teragrep.cfe_39.avro.SyslogRecord;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class KafkaConsumerTest {
    // Make sure application.properties has consumer.useMockKafkaConsumer=true enabled for Kafka testing.

    @Test
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


    @Test
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
        config.setMaximumFileSize(8000); // 10 loops (140 records) are in use at the moment, and that is sized at 36,102 bits.
        KafkaController kafkaController = new KafkaController(config);
        kafkaController.run();
        try {
            int counter = avroReader(1, 5);
            Assertions.assertEquals(140, counter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        cleanup(config, 1, 5);
    }

    // Reads the data from a list of avro files
    public int avroReader(int start, int end) throws IOException {
        // Deserialize Users from disk
        Config config = new Config();
        Path queueDirectory = Paths.get(config.getQueueDirectory());
        int counter = 0;
        int looper = 0;
        for (int i = start; i<=end; i++) {
            File syslogFile = new File(
                    queueDirectory.toAbsolutePath()
                            + File.separator
                            + config.getQueueNamePrefix()
                            + "."
                            + i
            );
            DatumReader<SyslogRecord> userDatumReader = new SpecificDatumReader<>(SyslogRecord.class);
            try (DataFileReader<SyslogRecord> dataFileReader = new DataFileReader<>(syslogFile, userDatumReader)) {
                SyslogRecord user = null;
                while (dataFileReader.hasNext()) {
                    user = dataFileReader.next(user);
                    System.out.println(user);
                    counter++;
                    looper++;
                    // All the mock data is generated from a set of 14 records.
                    if (looper <= 1) {
                        Assertions.assertEquals("[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!", user.getMessage().toString());
                    } else if (looper == 2) {
                        Assertions.assertEquals("[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!", user.getMessage().toString());
                    } else if (looper == 3) {
                        Assertions.assertEquals("470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.", user.getMessage().toString());
                    } else if (looper == 4) {
                        Assertions.assertEquals("470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.", user.getMessage().toString());
                    } else if (looper == 5) {
                        Assertions.assertEquals("470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.", user.getMessage().toString());
                    } else if (looper == 6) {
                        Assertions.assertEquals("25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]", user.getMessage().toString());
                    } else if (looper == 7) {
                        Assertions.assertEquals("25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]", user.getMessage().toString());
                    } else if (looper == 8) {
                        Assertions.assertEquals("25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]", user.getMessage().toString());
                    } else if (looper == 9) {
                        Assertions.assertEquals("25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]", user.getMessage().toString());
                    } else if (looper == 10) {
                        Assertions.assertEquals("25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]", user.getMessage().toString());
                    } else if (looper == 11) {
                        Assertions.assertEquals("25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]", user.getMessage().toString());
                    } else if (looper == 12) {
                        Assertions.assertEquals("25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]", user.getMessage().toString());
                    } else if (looper == 13) {
                        Assertions.assertEquals("25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]", user.getMessage().toString());
                    } else {
                        Assertions.assertEquals("25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]", user.getMessage().toString());
                        looper = 0;
                    }
                }
            }
        }
        System.out.println("Total number of records: " + counter);
        return counter;
    }

    // Deletes the avro-files that were created during testing.
    public void cleanup(Config config, int start, int end) {
        Path queueDirectory = Paths.get(config.getQueueDirectory());
        String queueNamePrefix = config.getQueueNamePrefix();
        for (int nextSequenceNumber = start; nextSequenceNumber <= end; nextSequenceNumber++) {
            File file = new File(
                    queueDirectory.toAbsolutePath()
                            + File.separator
                            + queueNamePrefix
                            + "."
                            + nextSequenceNumber
            );
            try {
                boolean result = Files.deleteIfExists(file.toPath()); //surround it in try catch block
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    // @Test
    public void kafkaConsumerTest() throws InterruptedException {
        // The mock kafka consumer is enabled in the config. Now it should be possible to implement tests using it. https://www.baeldung.com/kafka-mockconsumer
        // This code can be implemented as Main.main() function later.
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
        config.setMaximumFileSize(300000); // 10 loops are in use at the moment, and that is sized at 36,102 bits.
        // LOGGER.info("Running main program");
        KafkaController kafkaController = new KafkaController(config);
        kafkaController.run();
    }

    // Tests the serialization of the AVRO-file generated in kafkaConsumerTest(). Pathname depends on the configurations set in application.properties file.
    // @Test
    public void AVROReaderTest() throws IOException {
        // Deserialize Users from disk
        Config config = new Config();
        Path queueDirectory = Paths.get(config.getQueueDirectory());
        int counter = 0;
        for (int i = 5; i<=6; i++) {
            File syslogFile = new File(
                    queueDirectory.toAbsolutePath()
                            + File.separator
                            + config.getQueueNamePrefix()
                            + "."
                            + i
            );
            DatumReader<SyslogRecord> userDatumReader = new SpecificDatumReader<>(SyslogRecord.class);
            try (DataFileReader<SyslogRecord> dataFileReader = new DataFileReader<>(syslogFile, userDatumReader)) {
                SyslogRecord user = null;
                while (dataFileReader.hasNext()) {
                    user = dataFileReader.next(user);
                    System.out.println(user);
                    counter++;
                }
            }
        }
        System.out.println("Total number of records: " + counter);
    }
}
