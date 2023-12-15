package com.teragrep.cfe_39.consumers.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;

public class HDFSWriter implements AutoCloseable{

    // TODO: Add input parameters
    public HDFSWriter() {
        // TODO: Code for initializing the class
        // Add the code for sending the AVRO-serialized data to HDFS here, performance is measured between the start/end.
        //  Also remember to implement Kerberized access to HDFS.
        String hdfsuri = ""; // Get from config.

        String path="/user/hdfs/example/hdfs/";
        String fileName= recordOffsetObjectList+".csv"; // FIXME
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
    }

    // TODO: Add input parameters
    public void commit() {
        // TODO: code for committing the AVRO-file to HDFS
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
    }

    public void add() {
        // TODO: code for adding file for commit
    }

    public void close() {

    }


}
