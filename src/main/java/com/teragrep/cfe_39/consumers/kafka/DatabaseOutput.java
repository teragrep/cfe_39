package com.teragrep.cfe_39.consumers.kafka;

import com.teragrep.cfe_39.Config;
import com.teragrep.cfe_39.avro.SyslogRecord;
import com.teragrep.cfe_39.consumers.kafka.queue.WritableQueue;
import com.teragrep.cfe_39.metrics.topic.TopicCounter;
import com.teragrep.cfe_39.metrics.RuntimeStatistics;
import com.teragrep.rlo_06.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import java.nio.ByteBuffer;

import java.net.URI;

//  The kafka stream should first be deserialized using rlo_06 and then serialized again using avro and stored in HDFS.
//  The target where the record is stored in HDFS is based on the topic, partition and offset. ie. topic_name/0.123456 where offset is 123456

// TODO: make testcases using MockKafkaConsumerFactory. The mock consumer is activated for testing using the configuration file: readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")
// DONE:
// Thus the configuration file is the first thing to make for testcases, and the first testcases are for testing the configuration file.

public class DatabaseOutput implements Consumer<List<RecordOffsetObject>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseOutput.class);
    private final RFC5424Frame rfc5424Frame = new RFC5424Frame(false);

    private final String table;


    private final RuntimeStatistics runtimeStatistics;
    private final TopicCounter topicCounter;

    private long lastTimeCalled = Instant.now().toEpochMilli();

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_BLUE = "\u001B[34m";
    private SyslogAvroWriter syslogAvroWriter;
    private final long minimumFreeSpace;
    private final long maximumFileSize;
    private final WritableQueue writableQueue;
    private final ByteBuffer sourceConcatenationBuffer;
    private final SDVector teragrepStreamName;
    private final SDVector teragrepDirectory;
    private final SDVector eventNodeSourceSource;
    private final SDVector eventNodeRelaySource;
    private final SDVector eventNodeSourceSourceModule;
    private final SDVector eventNodeRelaySourceModule;
    private final SDVector eventNodeSourceHostname;
    private final SDVector eventNodeRelayHostname;
    private final SDVector originHostname;
    private File syslogFile;
    private final Config config;

    DatabaseOutput(
            Config config,
            String table,
            RuntimeStatistics runtimeStatistics,
            TopicCounter topicCounter
    ) {
        this.config = config;
        this.table = table;
        this.runtimeStatistics = runtimeStatistics;
        this.topicCounter = topicCounter;
        this.minimumFreeSpace = 32000000; // TODO: CHECK RIGHT VALUE FOR minimumFreeSpace
        this.maximumFileSize = 64000000; // Maximum file size should be 64M (64000000).

        // queueDirectory and queueNamePrefix shouldn't be critical to name according to the HDFS requirements (topic+partition+offset for filename) as it's just used for storing the AVRO-serialized files.
        this.writableQueue = new WritableQueue(
                config.getQueueDirectory(),
                config.getQueueNamePrefix()
        );

        this.sourceConcatenationBuffer = ByteBuffer.allocateDirect(256*1024);
        teragrepStreamName = new SDVector("teragrep@48577","streamname");
        teragrepDirectory = new SDVector("teragrep@48577","directory");
        this.eventNodeSourceSource = new SDVector("event_node_source@48577","source");
        this.eventNodeRelaySource = new SDVector("event_node_relay@48577","source");
        this.eventNodeSourceSourceModule = new SDVector("event_node_source@48577","source_module");
        this.eventNodeRelaySourceModule = new SDVector("event_node_relay@48577","source_module");
        this.eventNodeSourceHostname = new SDVector("event_node_source@48577","hostname");
        this.eventNodeRelayHostname = new SDVector("event_node_relay@48577","hostname");
        this.originHostname = new SDVector("origin@48577","hostname");
    }

    boolean checkSizeTooLarge(long fileSize, RecordOffsetObject lastObject) {
        try {
            // If the syslogAvroWriter is already initialized, check the filesize so it doesn't go above 64M.
            if (fileSize > maximumFileSize) {
                // file too large for adding the new record, write the still adequately sized AVRO-file to the HDFS database and create a new empty AVRO-file.

                // This part closes the writing of now "complete" AVRO-file and stores the file to HDFS.
                syslogAvroWriter.close();
                // TODO: RESTORE COMMENT BLOCK AFTER TESTING AVRO.
                /*try (HDFSWriter writer = new HDFSWriter(config, lastObject)) {
                    writer.commit(syslogFile); // commits the final AVRO-file to HDFS.
                }
                syslogFile.delete(); // Delete AVRO-files that have been committed to HDFS
                */

                // This part defines a new empty file to which the new AVRO-serialized records are stored until it again hits the 64M size limit.
                File syslogFile =
                        writableQueue.getNextWritableFile();
                syslogAvroWriter = new SyslogAvroWriter(syslogFile);
                return true;
            }
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
        return false;
    }

    private long rfc3339ToEpoch(ZonedDateTime zonedDateTime) {
        final Instant instant = zonedDateTime.toInstant();

        final long MICROS_PER_SECOND = 1000L * 1000L;
        final long NANOS_PER_MICROS = 1000L;
        final long sec = Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND);

        return Math.addExact(sec, instant.getNano() / NANOS_PER_MICROS);
    }

    // Input parameter is a list of RecordOffsetObjects. Each object contains a record and its metadata (topic, partition and offset).
    @Override
    public void accept(List<RecordOffsetObject> recordOffsetObjectList) {
        long thisTime = Instant.now().toEpochMilli();
        long ftook = thisTime - lastTimeCalled;
        topicCounter.setKafkaLatency(ftook);
        LOGGER.debug(ANSI_BLUE + "Fuura searching your batch for <[" + table + "]> with records <" + recordOffsetObjectList.size() + "> and took  <" + (ftook) + "> milliseconds. <" + (recordOffsetObjectList.size() * 1000L / ftook) + "> EPS. " + ANSI_RESET);
        long batchBytes = 0L;

        //  The recordOffsetObjectList loop will go through all the objects in the list.
        //  While it goes through the list, the contents of the objects are serialized into an AVRO-file.
        //  When the file size is about to go above 64M, commit the file into HDFS using the latest topic/partition/offset values as the filename and start fresh with an empty AVRO-file.
        //  Serialize the object that was going to make the file go above 64M into the now empty AVRO-file and continue the loop.
        // https://pagure.xnet.fi/com-teragrep/rlo_09/blob/avroness/f/src/main/java/com/teragrep/rlo_09/SyslogAvroWriter.java
        // https://pagure.xnet.fi/com-teragrep/rlo_09/blob/avroness/f/src/main/java/com/teragrep/rlo_09/WriteCoordinator.java
        // every recordOffsetObject.record on the recordOffsetObjectList basically represents a rlo_09 WriteCoordinator.accept(byte[] bytes) when the list is gone through in a loop.
        RecordOffsetObject lastObject = null;
        long start = Instant.now().toEpochMilli(); // Starts measuring performance here. Measures how long it takes to process the whole recordOffsetObjectList.
        for (RecordOffsetObject recordOffsetObject : recordOffsetObjectList) {
            lastObject = recordOffsetObject;
            // Initializing syslogAvroWriter.
            if (syslogAvroWriter == null) {
                try {
                    syslogFile =
                            writableQueue.getNextWritableFile();
                    //  The avro serialization filename shouldn't really matter as long as the name is changed when stuff is stored to HDFS.
                    //  And the content of the AVRO-serialized file that is going to be stored in HDFS is finalized only when the maximumFileSize has been reached.
                    //  This means the HDFS filename is only finalized when the AVRO-serialized file is finalized, because every Kafka-record added to the file is going to change the offset that is going to be used for the filename.
                    syslogAvroWriter = new SyslogAvroWriter(syslogFile);
                } catch (IOException ioException) {
                    throw new IllegalArgumentException(ioException);
                }
            } else {
                try {
                    checkSizeTooLarge(syslogAvroWriter.getFileSize(), lastObject);
                } catch (IOException ioException) {
                    throw new UncheckedIOException(ioException);
                }
            }

            byte[] byteArray = recordOffsetObject.record; // loads the byte[] contained in recordOffsetObject.record to byteArray.
            batchBytes = batchBytes + byteArray.length;
            InputStream inputStream = new ByteArrayInputStream(byteArray);
            rfc5424Frame.load(inputStream);
            try {
                if(rfc5424Frame.next()) {
                    // rfc5424Frame has loaded the record data, it's ready for deserialization.
                    //  Implement AVRO serialization for the Kafka records here, preparing the data for writing to HDFS.
                    //  Write all the data into a file using AVRO.
                    //  The size of each AVRO-serialized file should be as close to 64M as possible.

                    /*
                    EXAMPLE FORMAT FROM PTH_06.KafkaReconrdConverter WHICH SHOULD BE USED FOR AVRO-FORMAT!
                    return RowFactory.create(
                            Timestamp.from(instant),                    // 0 "_time", DataTypes.TimestampType
                            UTF8String.fromBytes(message).toString(),   // 1 "_raw", DataTypes.StringType
                            UTF8String.fromBytes(index).toString(),     // 2 "directory", DataTypes.StringType
                            UTF8String.fromBytes(sourcetype).toString(),// 3 "stream", DataTypes.StringType
                            UTF8String.fromBytes(hostname).toString(),  // 4 "host", DataTypes.StringType,
                            UTF8String.fromBytes(input).toString(),     // 5 "input", DataTypes.StringType
                            partition,                                  // 6 "partition", DataTypes.StringType
                            offset,                                     // 7 "offset", DataTypes.LongType
                            UTF8String.fromBytes(origin).toString()     // 8 "origin", DataTypes.StringType
                    );
                    */

                    // input
                    final byte[] source = eventToSource();

                    // origin
                    final byte[] origin = eventToOrigin();

                    // Format: Use AVRO format with syslog columns as indexed ones
                    final long epochMicros = rfc3339ToEpoch(new RFC5424Timestamp(rfc5424Frame.timestamp).toZonedDateTime());
                    SyslogRecord syslogRecord = SyslogRecord.newBuilder()
                            .setTimestamp(epochMicros)
                            .setMessage(rfc5424Frame.msg.toString())
                            .setDirectory(rfc5424Frame.structuredData.getValue(teragrepDirectory).toString())
                            .setStream(rfc5424Frame.structuredData.getValue(teragrepStreamName).toString()) // Or is sourcetype/stream supposed to be rfc5424Frame.appName.toString() instead?
                            .setHost(rfc5424Frame.hostname.toString())
                            .setInput(source.toString())
                            .setPartition(recordOffsetObject.partition.toString())
                            .setOffset(recordOffsetObject.offset)
                            .setOrigin(origin.toString())
                            .build();


                    // Calculate the size of syslogRecord that is going to be written to syslogAvroWriter-file.
                    long capacity = syslogRecord.toByteBuffer().capacity();
                    long capacitybefore = syslogAvroWriter.getFileSize(); // TODO: NOT WORKING PROPERLY! DOESN'T GIVE THE RIGHT FILE SIZE
                    // Check if there is still room in syslogAvroWriter for another syslogRecord. Commit syslogAvroWriter to HDFS if no room left, emptying it out in the process.
                    checkSizeTooLarge(syslogAvroWriter.getFileSize() + capacity, lastObject);
                    // Add syslogRecord to syslogAvroWriter which has rooom for new syslogRecord.
                    syslogAvroWriter.write(syslogRecord);

                    long capacityafter = syslogAvroWriter.getFileSize(); // TODO: NOT WORKING PROPERLY! DOESN'T GIVE THE RIGHT FILE SIZE
                    System.out.println("record capacity: " + capacity);
                    System.out.println("file capacity before adding record: " + capacitybefore);
                    System.out.println("file capacity after adding record: " + capacityafter);


                    /*new RFC5424Timestamp(rfc5424Frame.timestamp).toZonedDateTime().toInstant().getEpochSecond();
                    rfc5424Frame.appName.toString();
                    rfc5424Frame.hostname.toString();
                    rfc5424Frame.msg.toString();*/
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // Handle the "leftover" syslogRecords from the loop.
        try {
            if (syslogAvroWriter != null) {
                syslogAvroWriter.close();
                // TODO: RESTORE COMMENT BLOCK AFTER TESTING AVRO.
                /*try (HDFSWriter writer = new HDFSWriter(config, lastObject)) {
                    writer.commit(syslogFile); // commits the final AVRO-file to HDFS.
                }
                syslogFile.delete(); // Delete AVRO-files that have been committed to HDFS
                */
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Measures performance of code that is between start and end.
        long end = Instant.now().toEpochMilli();

        long took = (end - start);
        topicCounter.setDatabaseLatency(took);

        if (took == 0) {
            took = 1;
        }
        long rps = recordOffsetObjectList.size() * 1000L / took;
        topicCounter.setRecordsPerSecond(rps);

        long bps = batchBytes * 1000 / took;
        topicCounter.setBytesPerSecond(bps);

        runtimeStatistics.addAndGetRecords(recordOffsetObjectList.size());
        runtimeStatistics.addAndGetBytes(batchBytes);

        topicCounter.addToTotalBytes(batchBytes);
        topicCounter.addToTotalRecords(recordOffsetObjectList.size());

        LOGGER.debug(
                ANSI_GREEN
                        + "Sent batch for <[" + table + "]> "
                        + "with records <" + recordOffsetObjectList.size() + "> "
                        + "and size <" + batchBytes / 1024 + "> KB "
                        + "took <" + (took) + "> milliseconds. "
                        + "<" + rps + "> RPS. "
                        + "<" + bps / 1024 + "> KB/s "
                        + ANSI_RESET
        );
        lastTimeCalled = Instant.now().toEpochMilli();
    }



    private byte[] eventToOrigin() {
        byte[] origin;
        Fragment originFragment = rfc5424Frame.structuredData.getValue(originHostname);
        if (!originFragment.isStub) {
            origin = originFragment.toBytes();
        }
        else {
            origin = new byte[]{};
        }
        return origin;
    }

    private byte[] eventToSource() {
        //input is produced from SD element event_node_source@48577 by
        // concatenating "source_module:hostname:source". in case
        //if event_node_source@48577 is not available use event_node_relay@48577.
        //If neither are present, use null value.

        sourceConcatenationBuffer.clear();

        Fragment sourceModuleFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceSourceModule);
        if (sourceModuleFragment.isStub) {
            sourceModuleFragment = rfc5424Frame.structuredData.getValue(eventNodeRelaySourceModule);
        }

        byte[] source_module;
        if (!sourceModuleFragment.isStub) {
            source_module = sourceModuleFragment.toBytes();
        }
        else {
            source_module = new byte[]{};
        }

        Fragment sourceHostnameFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceHostname);
        if (sourceHostnameFragment.isStub) {
            sourceHostnameFragment = rfc5424Frame.structuredData.getValue(eventNodeRelayHostname);
        }

        byte[] source_hostname;
        if (!sourceHostnameFragment.isStub) {
            source_hostname = sourceHostnameFragment.toBytes();
        }
        else {
            source_hostname = new byte[]{};
        }


        Fragment sourceSourceFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceSource);
        if (sourceHostnameFragment.isStub) {
            sourceSourceFragment = rfc5424Frame.structuredData.getValue(eventNodeRelaySource);
        }

        byte[] source_source;
        if (!sourceSourceFragment.isStub) {
            source_source = sourceSourceFragment.toBytes();
        }
        else {
            source_source = new byte[]{};
        }


        // source_module:hostname:source"
        sourceConcatenationBuffer.put(source_module);
        sourceConcatenationBuffer.put((byte) ':');
        sourceConcatenationBuffer.put(source_hostname);
        sourceConcatenationBuffer.put((byte)':');
        sourceConcatenationBuffer.put(source_source);

        sourceConcatenationBuffer.flip();
        byte[] input = new byte[sourceConcatenationBuffer.remaining()];
        sourceConcatenationBuffer.get(input);

        return input;
    }
}