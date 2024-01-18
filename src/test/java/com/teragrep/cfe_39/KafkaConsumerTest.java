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
        config.setMaximumFileSize(8500); // 10 loops (140 records) are in use at the moment, and that is sized at 36,102 bits.
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
                int partitionCounter = 9; // The partitions are indexed from 0 to 9 when 10 loops are used in MockKafkaConsumerFactoryTemp.
                while (dataFileReader.hasNext()) {
                    user = dataFileReader.next(user);
                    System.out.println(syslogFile.getPath());
                    System.out.println(user);
                    counter++;
                    // All the mock data is generated from a set of 14 records.
                    /*if (looper <= 0) {
                        // FIXME: The partition ordering is wrong in kafkaconsumer. Must be fixed so the avro serialization works properly with correct filenames for HDFS.
                        Assertions.assertEquals("{\"timestamp\": 1650872090804000, \"message\": \"[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 0, \"origin\": \"jla-02.default\"}", user.toString());
                        looper++;
                    } else if (looper == 1) {
                        Assertions.assertEquals("{\"timestamp\": 1650872090806000, \"message\": \"[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 1, \"origin\": \"jla-02.default\"}", user.toString());
                        looper++;
                    } else if (looper == 2) {
                        Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 2, \"origin\": \"jla-02\"}", user.toString());
                        looper++;
                    } else if (looper == 3) {
                        Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 3, \"origin\": \"jla-02\"}", user.toString());
                        looper++;
                    } else if (looper == 4) {
                        Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 4, \"origin\": \"jla-02\"}", user.toString());
                        looper++;
                    } else if (looper == 5) {
                        Assertions.assertEquals("{\"timestamp\": 1650872092238000, \"message\": \"25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 5, \"origin\": \"jla-02.default\"}", user.toString());
                        looper++;
                    } else if (looper == 6) {
                        Assertions.assertEquals("{\"timestamp\": 1650872092239000, \"message\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 6, \"origin\": \"jla-02.default\"}", user.toString());
                        looper++;
                    } else if (looper == 7) {
                        Assertions.assertEquals("{\"timestamp\": 1650872092239000, \"message\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 7, \"origin\": \"jla-02.default\"}", user.toString());
                        looper++;
                    } else if (looper == 8) {
                        Assertions.assertEquals("{\"timestamp\": 1650872092240000, \"message\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 8, \"origin\": \"jla-02.default\"}", user.toString());
                        looper++;
                    } else if (looper == 9) {
                        Assertions.assertEquals("{\"timestamp\": 1650872092240000, \"message\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 9, \"origin\": \"jla-02.default\"}", user.toString());
                        looper++;
                    } else if (looper == 10) {
                        Assertions.assertEquals("{\"timestamp\": 1650872092241000, \"message\": \"25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 10, \"origin\": \"jla-02.default\"}", user.toString());
                        looper++;
                    } else if (looper == 11) {
                        Assertions.assertEquals("{\"timestamp\": 1650872092241000, \"message\": \"25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 11, \"origin\": \"jla-02.default\"}", user.toString());
                        looper++;
                    } else if (looper == 12) {
                        Assertions.assertEquals("{\"timestamp\": 1650872092242000, \"message\": \"25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 12, \"origin\": \"jla-02.default\"}", user.toString());
                        looper++;
                    } else {
                        Assertions.assertEquals("{\"timestamp\": 1650872092243000, \"message\": \"25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""+partitionCounter+"\", \"offset\": 13, \"origin\": \"jla-02.default\"}", user.toString());
                        looper = 0;
                        partitionCounter--;
                    }*/
                }
            }
        }
        System.out.println("Total number of records: " + counter);
        return counter;
    }

    // @Test
    public void debugger() {


        /*try {
            int counter = avroReader(1, 5);
            Assertions.assertEquals(140, counter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/

        // TODO: DEBUG THE PARTITION ORDER!
        // rewrite the key values for mock data generation.

        int amountofloops = 10; // number of loops for adding partitions/records to the mock consumer topic. Each loop adds a new partition of 14 records. 17777 loops results in file size slightly above 64M. 10 loops is sized at 36,102 bits.
        final MockConsumer<byte[], byte[]> consumer;
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        LinkedHashMap<TopicPartition, Long> beginningOffsets = new LinkedHashMap<>();
        LinkedHashMap<TopicPartition, Long> endOffsets = new LinkedHashMap<>();
        List<PartitionInfo> mockPartitionInfo = new ArrayList<>();
        // generate the topic partitions and metadata first
        for (int i = 0; i < amountofloops; i++) {
            TopicPartition topicPartition = new TopicPartition("testConsumerTopic", i);
            topicPartitions.add(topicPartition);
            beginningOffsets.put(topicPartition, 0L);
            endOffsets.put(topicPartition, 14L);
            mockPartitionInfo.add(new PartitionInfo("testConsumerTopic", i, null, null, null));
        }
        Collection<TopicPartition> testing = new ArrayList<>(topicPartitions);
        consumer.subscribe(Collections.singletonList("testConsumerTopic"));
        // consumer.assign(testing); // FIXME: PARTITIONS ARE IN WRONG ORDER IN kafkaConsumer, 7856341209. Should be 0123456789.

        consumer.updateBeginningOffsets(beginningOffsets);

        //insert stuff
        for (TopicPartition a : topicPartitions) {
            consumer.rebalance(Collections.singletonList(new TopicPartition("testConsumerTopic", 0)));
            generateEvents(consumer, a.topic(), a.partition()); // The ordering in this loop is fine, goes from 0 to 9 in correct order.
        }

        consumer.updateEndOffsets(endOffsets);
        consumer.updatePartitions("testConsumerTopic", mockPartitionInfo);


        // ASSERTIONS
        Set<TopicPartition> checkAssignmentOder = consumer.assignment(); // for testing only
        consumer.updateBeginningOffsets(beginningOffsets);
        int looper = 0;
        for (TopicPartition a : checkAssignmentOder) {
            Assertions.assertEquals(new TopicPartition("testConsumerTopic", looper), a);
            looper++;
        }



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
        config.setMaximumFileSize(3000); // 10 loops are in use at the moment, and that is sized at 36,102 bits.
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
        for (int i = 1; i<=20; i++) {
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
                System.out.println(syslogFile.getPath());
                while (dataFileReader.hasNext()) {
                    user = dataFileReader.next(user);
                    System.out.println(user);
                    counter++;
                }
            }
        }
        System.out.println("Total number of records: " + counter);
    }


    private static void generateEvents(MockConsumer<byte[], byte[]> consumer, String topicName, int partition) {
        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        0L,
                        "2022-04-25T07:34:50.804Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:50.804Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"835bf792-91cf-44e3-976b-518330bb8fd3\" source=\"source\" unixtime=\"1650872090805\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!".getBytes(StandardCharsets.UTF_8)
                )
        );
        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        1L,
                        "2022-04-25T07:34:50.806Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:50.806Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"c3f13f9a-05e2-41bd-b0ad-1eca6fd6fd9a\" source=\"source\" unixtime=\"1650872090806\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!".getBytes(StandardCharsets.UTF_8)
                )
        );
        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        2L,
                        "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"1848d8a1-2f08-4a1e-bec4-ff9e6dd92553\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        3L,
                        "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"5e1a0398-c2a0-468d-a562-c3bb31f0f853\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        4L,
                        "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"6268c3a2-5bda-427f-acce-29416eb445f4\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        5L,
                        "2022-04-25T07:34:52.238Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.238Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"b500dcaf-1101-4000-b6b9-bfb052ddbf86\" source=\"source\" unixtime=\"1650872092238\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        6L,
                        "2022-04-25T07:34:52.239Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.239Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"05363122-51ac-4c0b-a681-f5868081f56d\" source=\"source\" unixtime=\"1650872092239\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        7L,
                        "2022-04-25T07:34:52.239Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.239Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"7bbcd843-b795-4c14-b4a1-95f5d445cbcd\" source=\"source\" unixtime=\"1650872092239\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        8L,
                        "2022-04-25T07:34:52.240Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.240Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"2bc0a9f9-237d-4656-b40a-3038aace37f0\" source=\"source\" unixtime=\"1650872092240\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        9L,
                        "2022-04-25T07:34:52.240Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.240Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"ecf61e8d-e3a7-48ef-9b73-3c5a5243d2e6\" source=\"source\" unixtime=\"1650872092240\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        10L,
                        "2022-04-25T07:34:52.241Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.241Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"bf101d5a-e816-4f51-b132-97f8e3431f8e\" source=\"source\" unixtime=\"1650872092241\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        11L,
                        "2022-04-25T07:34:52.241Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.241Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"ef94d9e9-3c44-4892-b5a6-bf361d13ff97\" source=\"source\" unixtime=\"1650872092241\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        12L,
                        "2022-04-25T07:34:52.242Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.242Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"5bce6e3d-767d-44b4-a044-6c4872f8f2b5\" source=\"source\" unixtime=\"1650872092242\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );

        consumer.addRecord(new ConsumerRecord<>(topicName,
                        partition,
                        13L,
                        "2022-04-25T07:34:52.243Z".getBytes(StandardCharsets.UTF_8),
                        "<12>1 2022-04-25T07:34:52.243Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"3bb55ce4-0ea7-413a-b403-28b174d7ac99\" source=\"source\" unixtime=\"1650872092243\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]".getBytes(StandardCharsets.UTF_8)
                )
        );
    }
}
