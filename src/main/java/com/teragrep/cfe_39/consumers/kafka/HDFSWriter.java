package com.teragrep.cfe_39.consumers.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class HDFSWriter implements AutoCloseable{

    private String fileName;
    private String fileContent;
    private String path;
    private String hdfsuri;
    private Configuration conf;

    // TODO: Add input parameters, for example config.
    public HDFSWriter() {
        // TODO: Code for initializing the class
        //  Also remember to implement Kerberized access to HDFS.
        hdfsuri = ""; // Get from config.

        path="/user/hdfs/example/hdfs/";
        fileName= "test.csv"; // FIXME
        fileContent="hello;world";

        // ====== Init HDFS File System Object
        conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
    }

    // TODO: Add more input parameters that are needed for creating the proper filename for HDFS. At least Topic, Partition and Offset.
    public void commit(File syslogFile) {
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

    public void close() {

    }


}
