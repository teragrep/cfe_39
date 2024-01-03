package com.teragrep.cfe_39.consumers.kafka;

import com.teragrep.cfe_39.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

public class HDFSWriter implements AutoCloseable{

    private final String fileName;
    private final String path;
    private final FileSystem fs;

    public HDFSWriter(Config config, RecordOffsetObject lastObject) throws IOException {
        // Code for initializing the class
        // Also remember to implement Kerberized access to HDFS.
        String hdfsuri = config.getHdfsuri(); // Get from config.

        // The filepath should be something like hdfs:///opt/teragrep/cfe_39/srv/topic_name/0.12345 where 12345 is offset and 0 the partition.
        // The values are fetched from config and input parameters (topic+partition+offset).
        path = config.getHdfsPath()+"/"+lastObject.topic;
        fileName = lastObject.partition+"."+lastObject.offset; // filename should be constructed from partition and offset.


        // Set HADOOP user here, Kerberus parameters most likely needs to be added here too.
        System.setProperty("HADOOP_USER_NAME", "hdfs"); // TODO: Add to Config.java
        System.setProperty("hadoop.home.dir", "/"); // TODO: Add to Config.java

        // set kerberos host and realm
        System.setProperty("java.security.krb5.realm", "DRB.COM"); // TODO: Add to Config.java
        System.setProperty("java.security.krb5.kdc", "192.168.33.10"); // TODO: Add to Config.java

        Configuration conf = new Configuration();

        // enable kerberus
        conf.set("hadoop.security.authentication", "kerberos"); // TODO: Add to Config.java
        conf.set("hadoop.security.authorization", "true"); // TODO: Add to Config.java

        conf.set("fs.defaultFS", "hdfs://192.168.33.10"); // Set FileSystem URI  // TODO: Add to Config.java
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName()); // Maven stuff?
        conf.set("fs.file.impl", LocalFileSystem.class.getName()); // Maven stuff?

        // hack for running locally with fake DNS records
        // set this to true if overriding the host name in /etc/hosts
        conf.set("dfs.client.use.datanode.hostname", "true");  // TODO: Add to Config.java

        // server principal
        // the kerberos principle that the namenode is using
        conf.set("dfs.namenode.kerberos.principal.pattern", "hduser/*@DRB.COM");  // TODO: Add to Config.java

        // set usergroup stuff
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("dbathgate@DRB.COM", "src/main/resources/dbathgate.keytab");  // TODO: Add to Config.java

        // filesystem for HDFS access is set here
        fs = FileSystem.get(conf);
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

    // try-with-resources handles closing the filesystem automatically.
    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
