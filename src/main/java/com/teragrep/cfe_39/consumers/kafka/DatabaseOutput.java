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
import java.util.List;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import java.nio.ByteBuffer;

import java.net.URI;

// TODO:
//  The kafka stream should first be deserialized using rlo_06 and then serialized again using avro and stored in HDFS.
//  The target where the record is stored in HDFS is based on the topic, partition and offset. ie. topic_name/0.123456 where offset is 123456
//  First implement the AVRO serialization (saves the data into a file) and then implement the HDFS access for storing the data.

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
    private final SyslogRecord syslogRecord = new SyslogRecord();
    private SyslogAvroWriter syslogAvroWriter;
    private final long minimumFreeSpace;
    private final long maximumFileSize;
    private final WritableQueue writableQueue; // TODO: Implement the WritableQueue for managing the stream of records.

    DatabaseOutput(
            Config config,
            String table,
            RuntimeStatistics runtimeStatistics,
            TopicCounter topicCounter
    ) {
        this.table = table;
        this.runtimeStatistics = runtimeStatistics;
        this.topicCounter = topicCounter;
        this.minimumFreeSpace = 32; // TODO: CHECK RIGHT VALUE FOR minimumFreeSpace
        this.maximumFileSize = 64;
        // TODO: Extract queueDirectory and queueNamePrefix values from config!
        // queueDirectory (folder) is named based on topic name, something like this: hdfs:///opt/teragrep/cfe_39/srv/topic_name
        // queueNamePrefix is partition, for example 0
        // The end result that is produced in WritableQueue is hdfs:///opt/teragrep/cfe_39/srv/topic_name/0.12345 where 12345 is offset and 0 the partition.
        this.writableQueue = new WritableQueue(
                "queueDirectory",
                "queueNamePrefix"
        );
    }

    boolean checkSizeTooLarge(long fileSize) {
        try {
            // If the syslogAvroWriter is already initialized, check the filesize so it doesn't go above 64M.
            if (fileSize > maximumFileSize) {
                // file too large

                syslogAvroWriter.close();
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

    @Override
    public void accept(List<RecordOffsetObject> recordOffsetObjectList) {
        long thisTime = Instant.now().toEpochMilli();
        long ftook = thisTime - lastTimeCalled;
        topicCounter.setKafkaLatency(ftook);
        LOGGER.debug(ANSI_BLUE + "Fuura searching your batch for <[" + table + "]> with records <" + recordOffsetObjectList.size() + "> and took  <" + (ftook) + "> milliseconds. <" + (recordOffsetObjectList.size() * 1000L / ftook) + "> EPS. " + ANSI_RESET);
        long batchBytes = 0L;

        // TODO: The recordOffsetObjectList loop will go through all the objects in the list.
        //  While it goes through the list, the contents of the objects are serialized into an AVRO-file.
        //  When the file size is about to go above 64M, commit the file into HDFS using the latest topic/partition/offset values as the filename and start fresh with an empty AVRO-file.
        //  Serialize the object that was going to make the file go above 64M into the now empty AVRO-file and continue the loop.
        // https://pagure.xnet.fi/com-teragrep/rlo_09/blob/avroness/f/src/main/java/com/teragrep/rlo_09/SyslogAvroWriter.java
        // https://pagure.xnet.fi/com-teragrep/rlo_09/blob/avroness/f/src/main/java/com/teragrep/rlo_09/WriteCoordinator.java
        // every recordOffsetObject.record on the recordOffsetObjectList basically represents a rlo_09 WriteCoordinator.accept(byte[] bytes) when the list is gone through in a loop.
        for (RecordOffsetObject recordOffsetObject : recordOffsetObjectList) {

            // Initializing syslogAvroWriter.
            if (syslogAvroWriter == null) {
                try {
                    File syslogFile =
                            writableQueue.getNextWritableFile();
                    // TODO: Check how topic name, partition and offset should be added to the HDFS filename.
                    //  The avro serialization filename shouldn't really matter as long as the name is changed when stuff is stored to HDFS.
                    //  And the content of the AVRO-serialized file that is going to be stored in HDFS is finalized only when the maximumFileSize has been reached.
                    //  This means the HDFS filename is only finalized when the AVRO-serialized file is finalized, because every Kafka-record added to the file is going to change the offset that is going to be used for the filename.
                    syslogAvroWriter = new SyslogAvroWriter(syslogFile);
                } catch (IOException ioException) {
                    throw new IllegalArgumentException(ioException);
                }
            } else {
                try {
                    checkSizeTooLarge(syslogAvroWriter.getFileSize());
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
                    //  The size of each AVRO-serialized file should be as close to 64M as possible, and the name of the file should be set based on topic+partition+offset.

                    // TODO: Include stream (aka sourcetype) and directory (aka index) as well in syslogRecord. It only has record at the moment. Basically make the schema similar to that was used in mariadb...
                    syslogRecord.setContent(ByteBuffer.wrap(byteArray)); // byteArray is the loop's current recordOffsetObject.record
                    // Calculate the size of syslogRecord that is going to be written to syslogAvroWriter-file.
                    long capacity = syslogRecord.toByteBuffer().capacity();
                    checkSizeTooLarge(syslogAvroWriter.getFileSize() + capacity); // TODO: Fix the checkSizeTooLarge functionality so the closed syslogAvroWriter-file can be transferred to HDFS.
                    syslogAvroWriter.write(syslogRecord);

                    /*new RFC5424Timestamp(rfc5424Frame.timestamp).toZonedDateTime().toInstant().getEpochSecond();
                    rfc5424Frame.appName.toString();
                    rfc5424Frame.hostname.toString();
                    rfc5424Frame.msg.toString();*/
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // Handle possible "leftover" syslogRecords from the loop. TODO: Most likely borked like this, fix it in testing.
        try {
            if (syslogAvroWriter.getFileSize() > 0) {
                syslogAvroWriter.write(syslogRecord);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // TODO: BELOW STUFF IS GOING TO BE EITHER SCRAPPED OR MOVED TO ANOTHER METHOD WHICH IS THEN CALLED SEPARATELY! EVERYTHING SHOULD BE DONE WITHIN THE ABOVE LOOP!

        long start = Instant.now().toEpochMilli();

        // Add the code for sending the AVRO-serialized data to HDFS here, performance is measured between the start/end.
        //  Also remember to implement Kerberized access to HDFS.
        String hdfsuri = ""; // Get from config.

        String path="/user/hdfs/example/hdfs/";
        String fileName= recordOffsetObjectList+".csv";
        String fileContent="hello;world";

        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        //Get the filesystem - HDFS
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);

            //==== Create folder if not exists
            Path workingDir=fs.getWorkingDirectory();
            Path newFolderPath= new Path(path);
            if(!fs.exists(newFolderPath)) {
                // Create new Directory
                fs.mkdirs(newFolderPath);
                // logger.info("Path "+path+" created.");
            }

            //==== Write file
            // logger.info("Begin Write file into hdfs");
            //Create a path
            Path hdfswritepath = new Path(newFolderPath + "/" + fileName);
            //Init output stream
            FSDataOutputStream outputStream=fs.create(hdfswritepath);
            //Cassical output stream usage
            outputStream.writeBytes(fileContent);
            outputStream.close();
            // logger.info("End Write file into hdfs");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // TODO END

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
}