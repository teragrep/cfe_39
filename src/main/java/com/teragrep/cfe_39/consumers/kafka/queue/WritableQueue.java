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

package com.teragrep.cfe_39.consumers.kafka.queue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class WritableQueue {
    private final Path queueDirectory;
    private String queueNamePrefix;

    public WritableQueue(
            String queueDirectory
    ) {
        this.queueDirectory = Paths.get(queueDirectory);
        this.queueNamePrefix = "";
        QueueUtilities.accessCheck(this.queueDirectory);
    }

    private File getNextWritableFilename() throws IOException {

        try (Stream<Path> files = Files.find(
                queueDirectory,
                1,
                QueueUtilities.getFileMatcher(queueNamePrefix),
                FileVisitOption.FOLLOW_LINKS
        )) {

            long sequenceNumber = files.mapToLong(
                    QueueUtilities.getPathToSequenceNumberFunction()
            ).max().orElse(0);

            long nextSequenceNumber = sequenceNumber + 1;

            // create next
            return new File(
                    queueDirectory.toAbsolutePath()
                            + File.separator
                            + queueNamePrefix
                            + "."
                            + nextSequenceNumber
            );
        }
        catch (UncheckedIOException uncheckedIOException) {
            // just retry, reader modified the directory
            return getNextWritableFilename();
        }
    }

    public File getNextWritableFile() throws IOException {
        if (queueNamePrefix.isEmpty()){
            throw new IOException("No queueNamePrefix set");
        }else {
            return getNextWritableFilename();
        }
    }

    public void setQueueNamePrefix(String a) {
        this.queueNamePrefix = a;
    }
}