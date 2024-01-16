package com.teragrep.cfe_39.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.DynamicMBean;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

// TODO: Change DurationStatistics to use dropwizard instead of mxj_01.
public class DurationStatistics {
    MetricRegistry metricRegistry = new MetricRegistry(); // TODO: MetricRegistry is initialized here. Implement the different mxj_01 metrics objects to it.
    private static final Logger LOGGER = LoggerFactory.getLogger(DurationStatistics.class);
    private Instant lastReportTime = Instant.now();
    private long lastBytes = 0L;
    private long lastRecords = 0L;
    private final Counter samplingIntervalStatTemp = new Counter();
    private final Meter recordsPerSecondStatTemp = new Meter();
    private final Meter bytesPerSecondStatTemp = new Meter();
    private final Counter recordsTemp = new Counter();
    private final Counter bytesTemp = new Counter();
    private final Meter threadsStatTemp = new Meter();
    private final Meter bytesStatTemp = new Meter();
    private final Meter recordsStatTemp = new Meter();

    public MetricRegistry register() {
        // TODO: Register the different metrics to metricRegistry here.
        metricRegistry.register("samplingIntervalStat", samplingIntervalStatTemp);
        metricRegistry.register("recordsPerSecondStat", recordsPerSecondStatTemp);
        metricRegistry.register("bytesPerSecondStat", bytesPerSecondStatTemp);
        metricRegistry.register("records", recordsTemp);
        metricRegistry.register("bytes", bytesTemp);
        metricRegistry.register("threadsStat", threadsStatTemp);
        metricRegistry.register("bytesStat", bytesStatTemp);
        metricRegistry.register("recordsStat", recordsStatTemp);
        return metricRegistry;
    }

    // TODO: Alter the function like in RunTimeStatistics, so the class is only using dropwizard instead of mxj_01
    public void report() {
        long currentRecords = addAndGetRecords(0); // gets the total number of records processed during the current loop AND the previous loops.
        System.out.println("currentRecords: "+currentRecords);
        long currentBytes = addAndGetBytes(0);// gets the total amount of bytes processed during the current loop AND the previous loops.

        // Check if new records were processed
        if (currentRecords > lastRecords) {
            recordsTemp.inc(currentRecords - lastRecords); // new records found, adding the number of records to recordsTemp.
        }else {
            long current = recordsTemp.getCount();
            recordsTemp.dec(current); // no new records so set the counter back to 0.
        }
        if (currentBytes > lastBytes) {
            bytesTemp.inc(currentBytes - lastBytes); // new records found, adding the number of records to recordsTemp.
        }else {
            long current = bytesTemp.getCount();
            bytesTemp.dec(current); // no new records so set the counter back to 0.
        }

        Instant currentTime = Instant.now();
        long took = currentTime.toEpochMilli() - lastReportTime.toEpochMilli();
        samplingIntervalStatTemp.inc(took);

        recordsPerSecondStatTemp.mark(currentRecords-lastRecords);
        bytesPerSecondStatTemp.mark(currentBytes-lastBytes);

        // persist
        lastReportTime = currentTime;
        lastRecords = currentRecords;
        lastBytes = currentBytes;
    }

    public long getTotalRecords() {
        return recordsTemp.getCount();
    }

    public void log() {
        LOGGER.info(
                "## Processed records <" + recordsTemp.getCount() + "> " +
                        "and size <" + bytesTemp.getCount() / 1024 + "> KB " +
                        "during <" + samplingIntervalStatTemp.getCount() + "> ms / " +
                        "Rates for past minute: <" + recordsPerSecondStatTemp.getOneMinuteRate() + "> RPS. " +
                        "<" + bytesPerSecondStatTemp.getOneMinuteRate() / 1024 + "> KB/s "
        );
        samplingIntervalStatTemp.dec(samplingIntervalStatTemp.getCount());
    }
    public long addAndGetThreads(long delta) {
        threadsStatTemp.mark(delta);
        return threadsStatTemp.getCount();
        // return threadsStat.getAtomicLong().addAndGet(delta);
    }

    public long addAndGetBytes(long delta) {
        bytesStatTemp.mark(delta);
        return bytesStatTemp.getCount();
        // return bytesStat.getAtomicLong().addAndGet(delta);
    }

    public long addAndGetRecords(long delta) {
        recordsStatTemp.mark(delta);
        return recordsStatTemp.getCount();
        // return recordsStat.getAtomicLong().addAndGet(delta);
    }
}
