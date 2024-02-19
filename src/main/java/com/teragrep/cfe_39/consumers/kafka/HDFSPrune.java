/*
   HDFS Data Ingestion for PTH_06 use CFE-39
   Copyright (C) 2022  Fail-Safe IT Solutions Oy

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.teragrep.cfe_39.consumers.kafka;

import com.teragrep.cfe_39.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class HDFSPrune {
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSPrune.class);
    private Config config;
    private final FileSystem fs;
    private Path newFolderPath;
    private long cutoff_epoch;

    public HDFSPrune(Config config, String topicName) throws IOException {
        this.config = config;
        String hdfsuri = config.getHdfsuri();

        String path = config.getHdfsPath()+"/"+topicName;
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
        fs = FileSystem.get(URI.create(hdfsuri), conf);

        //==== Create folder if not exists
        Path workingDir=fs.getWorkingDirectory();
        newFolderPath= new Path(path);
        if(!fs.exists(newFolderPath)) {
            // Create new Directory
            fs.mkdirs(newFolderPath);
            LOGGER.info("Path "+path+" created.");
        }

        cutoff_epoch = System.currentTimeMillis() - 172800000L; // TODO: cutoff offset is 172800000L only for testing, parametrize it using Config.java.
    }

    public void prune() throws IOException {
        // Fetch the filestatuses of HDFS files.
        FileStatus[] fileStatuses = fs.listStatus(new Path(newFolderPath + "/"));
        for (FileStatus a : fileStatuses) {
            // If all the files have their modification timestamp altered to mirror the final record timestamp, it is possible to prune the database based on the timestamps of the fileStatuses object.
            long convert = TimeUnit.MILLISECONDS.convert(a.getModificationTime(), TimeUnit.MICROSECONDS); // MICROSECONDS ARE NOT SUPPORTED, convert the microsecond epoch to milliseconds.
            // Delete old files
            if (convert < cutoff_epoch) {
                boolean delete = fs.delete(a.getPath(), true);
                LOGGER.info("Deleted file " + a.getPath());
            }
        }
    }
}
