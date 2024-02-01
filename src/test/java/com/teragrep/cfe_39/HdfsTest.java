package com.teragrep.cfe_39;

import com.teragrep.cfe_39.avro.SyslogRecord;
import com.teragrep.cfe_39.consumers.kafka.KafkaController;
import com.teragrep.cfe_39.consumers.kafka.RecordOffsetObject;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;

public class HdfsTest {

    // Generate AVRO-files for testing the HDFS writes.
    @BeforeAll
    public static void generateTestData() throws IOException, InterruptedException {
        Config config = new Config();
        config.setMaximumFileSize(3000); // 10 loops (140 records) are in use at the moment, and that is sized at 36,102 bits.
        KafkaController kafkaController = new KafkaController(config);
        kafkaController.run();
    }

    // Delete the generated AVRO-files.
    @AfterAll
    public static void deleteTestData() throws IOException {
        Config config = new Config();
        Path queueDirectory = new Path(config.getQueueDirectory()); // Paths.get(config.getQueueDirectory());
        for (int j = 0; j <= 9; j++) {
            for (int i = 1; i <= 5; i++) {
                File syslogFile = new File(
                        queueDirectory.toUri()
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

    @Test
    public void hdfsWriteTest() {
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

        Path queueDirectory = new Path(config.getQueueDirectory());
        for (int j = 0; j <= 9; j++) {
            for (int i = 1; i <= 2; i++) {
                File syslogFile = new File(
                        queueDirectory.toUri()
                                + File.separator
                                + "testConsumerTopic"
                                + j
                                + "."
                                + i
                );

                // generate lastObject from the last record in the file in this test
                DatumReader<SyslogRecord> userDatumReader = new SpecificDatumReader<>(SyslogRecord.class);
                SyslogRecord lastRecord = null;
                try (DataFileReader<SyslogRecord> dataFileReader = new DataFileReader<>(syslogFile, userDatumReader)) {
                    while (dataFileReader.hasNext()) {
                        lastRecord = dataFileReader.next(lastRecord);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                assert lastRecord != null;
                RecordOffsetObject lastObject = new RecordOffsetObject("testConsumerTopic", Integer.parseInt(lastRecord.getPartition().toString()), lastRecord.getOffset(), null); // Fetch input parameters from the lastRecord SyslogRecord-object.
                System.out.println("Last record in the " + syslogFile.getName() + " file:" + "\ntopic: " + lastObject.topic + "\npartition: " + lastObject.partition + "\noffset: " + lastObject.offset + "\n");
                /*try (HDFSWriter writer = new HDFSWriter(config, lastObject)) {
                    writer.commit(syslogFile); // commits the final AVRO-file to HDFS.
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }*/

                // Check that the file was stored to HDFS properly.
                try {
                    wait(1000);
                    hdfsReadCheck("testConsumerTopic", Integer.parseInt(lastRecord.getPartition().toString()), lastRecord.getOffset());
                } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }

            }
        }
    }

    public void hdfsReadCheck(String testConsumerTopic, int partition, long offset) throws IOException {

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

        // Check that the files were properly written to HDFS with a read test.
        String hdfsuri = config.getHdfsuri();

        String path = config.getHdfsPath()+"/"+testConsumerTopic;
        String fileName = partition+"."+offset;
        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        //Get the filesystem - HDFS
        FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);

        //==== Create folder if not exists
        Path workingDir=fs.getWorkingDirectory();
        Path newFolderPath= new Path(path);
        if(!fs.exists(newFolderPath)) {
            // Create new Directory
            fs.mkdirs(newFolderPath);
            // logger.info("Path "+path+" created.");
        }

        // This is the HDFS write path for the files:
        // Path hdfswritepath = new Path(newFolderPath + "/" + fileName); where newFolderPath is config.getHdfsPath() + "/" + lastObject.topic; and filename is lastObject.partition+"."+lastObject.offset;

        //==== Read files
        // logger.info("Read file into hdfs");
        //Create a path
        Path hdfsreadpath = new Path(newFolderPath + "/" + fileName); // The path should be the same that was used in writing the file to HDFS.
        //Init input stream
        FSDataInputStream inputStream = fs.open(hdfsreadpath);
        //The data is in AVRO-format, so it can't be read as a string.
        DataFileStream<SyslogRecord> reader = new DataFileStream<>(inputStream, new SpecificDatumReader<>(SyslogRecord.class));
        SyslogRecord record = null;
        while (reader.hasNext()) {
            reader.next();
            record = reader.next(record);
            System.out.println(record);
            // Assert records here like it is done in KafkaConsumerTest.avroReader().
            if (offset <= 0) {
                Assertions.assertEquals("{\"timestamp\": 1650872090804000, \"message\": \"[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 0, \"origin\": \"jla-02.default\"}", record.toString());
            } else if (offset == 1) {
                Assertions.assertEquals("{\"timestamp\": 1650872090806000, \"message\": \"[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 1, \"origin\": \"jla-02.default\"}", record.toString());
            } else if (offset == 2) {
                Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 2, \"origin\": \"jla-02\"}", record.toString());
            } else if (offset == 3) {
                Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 3, \"origin\": \"jla-02\"}", record.toString());
            } else if (offset == 4) {
                Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 4, \"origin\": \"jla-02\"}", record.toString());
            } else if (offset == 5) {
                Assertions.assertEquals("{\"timestamp\": 1650872092238000, \"message\": \"25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 5, \"origin\": \"jla-02.default\"}", record.toString());
            } else if (offset == 6) {
                Assertions.assertEquals("{\"timestamp\": 1650872092239000, \"message\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 6, \"origin\": \"jla-02.default\"}", record.toString());
            } else if (offset == 7) {
                Assertions.assertEquals("{\"timestamp\": 1650872092239000, \"message\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 7, \"origin\": \"jla-02.default\"}", record.toString());
            } else if (offset == 8) {
                Assertions.assertEquals("{\"timestamp\": 1650872092240000, \"message\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 8, \"origin\": \"jla-02.default\"}", record.toString());
            } else if (offset == 9) {
                Assertions.assertEquals("{\"timestamp\": 1650872092240000, \"message\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 9, \"origin\": \"jla-02.default\"}", record.toString());
            } else if (offset == 10) {
                Assertions.assertEquals("{\"timestamp\": 1650872092241000, \"message\": \"25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 10, \"origin\": \"jla-02.default\"}", record.toString());
            } else if (offset == 11) {
                Assertions.assertEquals("{\"timestamp\": 1650872092241000, \"message\": \"25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 11, \"origin\": \"jla-02.default\"}", record.toString());
            } else if (offset == 12) {
                Assertions.assertEquals("{\"timestamp\": 1650872092242000, \"message\": \"25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 12, \"origin\": \"jla-02.default\"}", record.toString());
            } else {
                Assertions.assertEquals("{\"timestamp\": 1650872092243000, \"message\": \"25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 13, \"origin\": \"jla-02.default\"}", record.toString());
            }

        }
        // logger.info(out);
        inputStream.close();
        fs.close();
    }
}
