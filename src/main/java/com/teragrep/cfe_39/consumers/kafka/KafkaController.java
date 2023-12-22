package com.teragrep.cfe_39.consumers.kafka;


import com.teragrep.cfe_39.Config;
import com.teragrep.cfe_39.metrics.*;
import com.teragrep.cfe_39.metrics.topic.TopicCounter;
import com.teragrep.cfe_39.metrics.topic.TopicStatistics;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaController {
    // rlo_09 KafkaReader is the code that I should take a look at. ReadCoordinator alone won't allow access to the kafka offsets, it must be done in KafkaReader.
    // ReadCoordinator uses the KafkaReader, but it's set as private and there are no functions for accessing it through ReadCoordinator.
    // The enable.auto.commit=false is set in config and it is fetched by the config.getKafkaConsumerProperties().
    // cfe_30 is already using enable.auto.commit=false, so looking through cfe_30 and rlo_09 ReadCoordinator and KafkaReader functions should get the coding on right track.

    // A consumer may opt to commit offsets by itself (enable.auto.commit=false).
    // Depending on when it chooses to commit offsets, there are delivery semantics available to the consumer.
    // Exactly once:
    // - For Kafka topic to External System workflows, to effectively achieve exactly once, you must use an idempotent consumer.

    // An Idempotent Consumer pattern uses a Kafka consumer that can consume the same message any number of times, but only process it once.
    // To implement the Idempotent Consumer pattern the recommended approach is to add a table to the database to track processed messages.
    // Each message needs to have a unique messageId assigned by the producing service, either within the payload, or as a Kafka message header.
    // When a new message is consumed the table is checked for the existence of this message Id. If present, then the message is a duplicate.
    // The consumer updates its offsets to effectively mark the message as consumed to ensure it is not redelivered, and no further action takes place.
    // If the message Id is not present in the table then a database transaction is started and the message Id is inserted.
    // The message is then processed performing the required business logic. Upon completion the transaction is committed.

    // requirements: SKIPPING IDEMPOTENT IMPLEMENTATION FOR NOW!
    // 1. The KafkaReader-class must be able to pass the offset values alongside the consumed message to the main class that is calling KafkaReader.
    // 2. The main class must handle the consuming of the kafka topics in an idempotent way as stated above.
    // This is achieved by using the HDFS filenames to store the topic_name and offset values of Kafka topics.
    // In other words Kafka consumers will consume topics normally according to the offsets that Kafka stores internally,
    // but the processor of the consumed messages will check the offsets of the messages that are consumed by kafka and
    // processes ONLY those messages that have not already been processed based on the offset values stored in HDFS filenames.

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaController.class);

    private final Config config;

    private final org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> kafkaConsumer;
    private final List<Thread> threads = new ArrayList<>();
    private final Set<String> activeTopics = new HashSet<>();
    private final RuntimeStatistics runtimeStatistics = new RuntimeStatistics();

    public KafkaController(Config config) {
        this.config = config;
        Properties readerKafkaProperties = config.getKafkaConsumerProperties();
        boolean useMockKafkaConsumer = Boolean.parseBoolean(
                readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")
        );
        if (useMockKafkaConsumer) {
            this.kafkaConsumer = MockKafkaConsumerFactoryTemp.getConsumer();
        } else {
            this.kafkaConsumer = new KafkaConsumer<>(config.getKafkaConsumerProperties(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
        }
    }

    public void run() throws InterruptedException {
        // register runtime statistics
        // runtimeStatistics.register(); // FIXME

        // register duration statistics
        DurationStatistics durationStatistics = new DurationStatistics();
        // durationStatistics.register(); // FIXME

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();
        TopicStatistics topicMetrics = new TopicStatistics(
                topicCounters
        );
        topicMetrics.register();

        while (true) {
            LOGGER.debug("Scanning for threads");
            topicScan(durationStatistics, topicCounters);

            // log stuff
            durationStatistics.log();
            long topicScanDelay = 30000L;
            Thread.sleep(topicScanDelay);
        }
    }

    // Creates kafka topic consumer based on input parameters.
    private void createReader(String topic, List<TopicCounter> topicCounters) throws SQLException {

        // Create a new topicCounter object for the topic that has not been added to topicCounters-list yet.
        TopicCounter topicCounter = new TopicCounter(topic);
        // Add the new topicCounter object to the list.
        topicCounters.add(topicCounter);

        // DatabaseOutput handles transferring the consumed data to storage (S3, mariadb, HDFS, etc.)
        // Kafka offset tracking must be included here.
        // Topic is figured out in topicScan so the offsets for the topic should be figured out here.
        Consumer<List<RecordOffsetObject>> output = new DatabaseOutput(
                config, // Configuration settings
                topic, // String, the name of the topic
                runtimeStatistics, // RuntimeStatistics object from metrics
                topicCounter // TopicCounter object from metrics
        );

        // The kafka offsets must be passed to HDFS. The consumer must also be set to manual commits so the HDFS can handle managing the commit offsets within the HDFS filenames.
        // plain rlo_09.ReadCoordinator won't give access to offset values. Implementing custom rlo_09 code in the package to achieve offset access.
        ReadCoordinatorTemp readCoordinator = new ReadCoordinatorTemp(
                topic,
                config.getKafkaConsumerProperties(),
                output
        );

        // Every consumer is run in a separate thread.
        Thread readThread = new Thread(null, readCoordinator, topic);
        threads.add(readThread);
        readThread.start();
    }

    private void topicScan(DurationStatistics durationStatistics, List<TopicCounter> topicCounters) {
        Map<String, List<PartitionInfo>> listTopics = kafkaConsumer.listTopics(Duration.ofSeconds(60)); // TODO: The listTopics is empty, this means problems in mock kafka.
        Pattern topicsRegex = Pattern.compile(config.getQueueTopicPattern());

        // Find the topics available in Kafka based on given QueueTopicPattern, both active and in-active.
        // Check how partitions are handled, need to allow using consumer groups for partition read assignments. aka. load balancing
        Set<String> foundTopics = new HashSet<>();
        for (Map.Entry<String, List<PartitionInfo>> entry : listTopics.entrySet()) {
            Matcher matcher = topicsRegex.matcher(entry.getKey());
            if (matcher.matches()) {
                foundTopics.add(entry.getKey());
            }
        }

        if (foundTopics.isEmpty()) {
            throw new IllegalStateException("Pattern <[" + config.getQueueTopicPattern() + "]> found no topics." );
        }

        // subtract currently active topics from found topics
        foundTopics.removeAll(activeTopics);

        // Activate all the found in-active topics, in other words create individual consumers for all of them using the createReader()-function.
        for (String topic : foundTopics) {
            LOGGER.info("Activating topic <"+topic+">");
            try {
                createReader(topic, topicCounters);
                activeTopics.add(topic);
                runtimeStatistics.addAndGetThreads(1);
            }
            catch (SQLException sqlException) {
                LOGGER.error("Topic <"+topic+"> not activated due to reader creation error: " + sqlException);
            }
        }
        durationStatistics.report(runtimeStatistics);
    }

}
