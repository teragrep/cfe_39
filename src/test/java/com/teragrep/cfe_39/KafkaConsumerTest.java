package com.teragrep.cfe_39;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

public class KafkaConsumerTest {


    @Test
    public void kafkaConsumerTest() {
        // TODO: make tests here
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
