package com.teragrep.cfe_39;

import com.teragrep.cfe_39.consumers.kafka.KafkaController;
import org.junit.jupiter.api.Test;

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
        kafkaController.run(); // FIXME: java.lang.IllegalStateException: Pattern <[testConsumerTopic]> found no topics. at com.teragrep.cfe_39.consumers.kafka.KafkaController.topicScan(KafkaController.java:146)
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
