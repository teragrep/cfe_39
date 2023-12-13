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
    private final String queueNamePrefix;

    public WritableQueue(
            String queueDirectory,
            String queueNamePrefix
    ) {
        this.queueDirectory = Paths.get(queueDirectory);
        this.queueNamePrefix = queueNamePrefix;
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
        return getNextWritableFilename();
    }
}