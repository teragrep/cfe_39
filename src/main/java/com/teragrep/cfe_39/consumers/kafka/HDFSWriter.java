package com.teragrep.cfe_39.consumers.kafka;

import com.teragrep.cfe_39.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

public class HDFSWriter implements AutoCloseable{

    private final String fileName;
    private final String path;
    private final FileSystem fs;

    public HDFSWriter(Config config, RecordOffsetObject lastObject) {
        // Code for initializing the class
        // Also remember to implement Kerberized access to HDFS.
        String hdfsuri = config.getHdfsuri(); // Get from config.

        // The filepath should be something like hdfs:///opt/teragrep/cfe_39/srv/topic_name/0.12345 where 12345 is offset and 0 the partition.
        // These values should be fetched from config and other input parameters (topic+partition+offset).
        path = config.getHdfsPath()+"/"+lastObject.topic;
        fileName = lastObject.partition+"."+lastObject.offset; // filename should be constructed from partition and offset.

        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        // Set HADOOP user here, Kerberus parameters most likely needs to be added here too.
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        // filesystem for HDFS access is set here
        try {
            fs = FileSystem.get(URI.create(hdfsuri), conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Method for committing the AVRO-file to HDFS
    public void commit(File syslogFile) {
        //Get the filesystem - HDFS
        try {
            //==== Create folder if not exists
            Path workingDir=fs.getWorkingDirectory();
            // Sets the directory where the data should be stored, if the directory doesn't exist then it's created.
            Path newFolderPath= new Path(path);
            if(!fs.exists(newFolderPath)) {
                // Create new Directory
                fs.mkdirs(newFolderPath);
                // logger.info("Path "+path+" created.");
            }

            //==== Write file
            // logger.info("Begin Write file into hdfs");
            //Create a path
            Path hdfswritepath = new Path(newFolderPath + "/" + fileName); // filename should be set according to the requirements: 0.12345 where 0 is Kafka partition and 12345 is Kafka offset.
            //Init output stream
            FSDataOutputStream outputStream=fs.create(hdfswritepath);
            // Write the file contents of syslogFile to hdfswritepath in HDFS.
            // file to bytes[]
            byte[] bytes = Files.readAllBytes(Paths.get(syslogFile.getPath()));
            outputStream.write(bytes);
            outputStream.close();
            // logger.info("End Write file into hdfs");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
