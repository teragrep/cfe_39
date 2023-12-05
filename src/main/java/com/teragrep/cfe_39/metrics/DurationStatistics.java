package com.teragrep.cfe_39.metrics;

import com.teragrep.cfe_39.metrics.mxj.AtomicLongMXJItem;
import com.teragrep.cfe_39.metrics.mxj.MXJBeanDynamizer;
import com.teragrep.cfe_39.metrics.mxj.MXJEndpoint;
import com.teragrep.cfe_39.metrics.mxj.MXJItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.DynamicMBean;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class DurationStatistics {
    private static final Logger LOGGER = LoggerFactory.getLogger(DurationStatistics.class);
    private final AtomicLongMXJItem samplingIntervalStat = new AtomicLongMXJItem("samplingInterval");
    private final AtomicLongMXJItem recordsPerSecondStat = new AtomicLongMXJItem("recordsPerSecond");
    private final AtomicLongMXJItem bytesPerSecondStat = new AtomicLongMXJItem("bytesPerSecond");
    private final AtomicLongMXJItem bytes = new AtomicLongMXJItem("bytes");
    private final AtomicLongMXJItem records = new AtomicLongMXJItem("records");
    private Instant lastReportTime = Instant.now();
    private long lastBytes = 0L;
    private long lastRecords = 0L;

    public DynamicMBean register() {
        List<MXJItem> itemList = new ArrayList<>();
        itemList.add(samplingIntervalStat);
        itemList.add(recordsPerSecondStat);
        itemList.add(bytesPerSecondStat);

        MXJBeanDynamizer dynamizer = new MXJBeanDynamizer(itemList);

        MXJEndpoint mxjEndpoint = new MXJEndpoint(
                "com.teragrep.cfe_39",
                "Metrics",
                "DurationTotals",
                dynamizer.createDynamicMBean()
        );

        return mxjEndpoint.register();
    }

    public void report(RuntimeStatistics runtimeStatistics) {
        long currentRecords = runtimeStatistics.addAndGetRecords(0);
        long currentBytes = runtimeStatistics.addAndGetBytes(0);

        long durationRecords = currentRecords - lastRecords;
        long durationBytes = currentBytes - lastBytes;

        records.getSetter().accept(durationRecords);
        bytes.getSetter().accept(durationBytes);

        Instant currentTime = Instant.now();
        long took = currentTime.toEpochMilli() - lastReportTime.toEpochMilli();

        samplingIntervalStat.getSetter().accept(took);

        if (took == 0) {
            took = 1;
        }

        recordsPerSecondStat.getSetter().accept(durationRecords * 1000L / took);
        bytesPerSecondStat.getSetter().accept(durationBytes * 1000L / took);

        // persist
        lastReportTime = currentTime;
        lastRecords = currentRecords;
        lastBytes = currentBytes;
    }

    public void log() {
        LOGGER.info(
                "## TOTAL records <" + records.getReader().get() + "> " +
                        "and size <" + bytes.getReader().get() / 1024 + "> KB " +
                        "<" + recordsPerSecondStat.getReader().get() + "> RPS. " +
                        "<" + bytesPerSecondStat.getReader().get() / 1024 + "> KB/s " +
                        "during <" + samplingIntervalStat.getReader().get() + "> ms"
        );
    }
}
