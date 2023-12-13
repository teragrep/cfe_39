package com.teragrep.cfe_39.consumers.kafka.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.BiPredicate;
import java.util.function.ToLongFunction;

class QueueUtilities {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(QueueUtilities.class);

    static BiPredicate<Path, BasicFileAttributes> getFileMatcher(String queueNamePrefix) {
        return (path, basicFileAttributes) -> {
            if (!path.getFileName().toString().startsWith(queueNamePrefix)) {
                return false;
            } else if (path.getFileName().toString().endsWith(".state")) {
                return false;
            } else if (!basicFileAttributes.isRegularFile()) {
                return false;
            } else {
                LOGGER.trace("getFileMatcher returning: " + path);
                return true;
            }
        };
    }

    static void accessCheck(Path queueDirectory) {
        if (!Files.isDirectory(queueDirectory)) {
            throw new IllegalArgumentException(
                    "Provided path is not a "
                            + "directory <[" + queueDirectory + "]>");
        }

        if (!Files.isWritable(queueDirectory)) {
            throw new IllegalArgumentException(
                    "Provided path is not "
                            + "writeable <[" + queueDirectory + "]>");
        }
    }

    static ToLongFunction<Path> getPathToSequenceNumberFunction() {
        return path -> {
            String pathString = path.toString();

            int dotPosition = pathString.lastIndexOf('.');

            String sequenceNumberString = pathString.substring(dotPosition + 1);

            return Long.parseLong(sequenceNumberString);
        };
    }
}