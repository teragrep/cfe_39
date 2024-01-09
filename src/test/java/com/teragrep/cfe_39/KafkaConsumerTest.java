package com.teragrep.cfe_39;

import com.teragrep.cfe_39.consumers.kafka.KafkaController;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.Test;
import com.teragrep.cfe_39.avro.SyslogRecord;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class KafkaConsumerTest {

    // TODO: make tests here. Make sure application.properties has consumer.useMockKafkaConsumer=true enabled for Kafka testing.
    @Test
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
        // LOGGER.info("Running main program");
        KafkaController kafkaController = new KafkaController(config);
        kafkaController.run(); // TODO: Everything is working until kafkaController.topicScan(). AVRO serialization also seems to work well. Now moving to implementing HDFS database and testing it.
    }

    // Tests the serialization of the AVRO-file generated in kafkaConsumerTest(). Pathname depends on the configurations set in application.properties file.
    @Test
    public void AVROReaderTest() throws IOException {
        // Deserialize Users from disk
        Config config = new Config();
        Path queueDirectory = Paths.get(config.getQueueDirectory());
        File syslogFile = new File(
                queueDirectory.toAbsolutePath()
                        + File.separator
                        + config.getQueueNamePrefix()
                        + "."
                        + 1 // change value if there are more than one avro-file generated etc.
        );;
        int counter = 0;
        DatumReader<SyslogRecord> userDatumReader = new SpecificDatumReader<>(SyslogRecord.class);
        try (DataFileReader<SyslogRecord> dataFileReader = new DataFileReader<>(syslogFile, userDatumReader)) {
            SyslogRecord user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user);
                counter++;
            }
        }
        System.out.println("Total number of records: " + counter);
    }

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
}
