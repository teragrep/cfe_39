package com.teragrep.cfe_39.metrics;


import com.teragrep.cfe_39.metrics.mxj.AtomicLongMXJItem;
import com.teragrep.cfe_39.metrics.mxj.MXJBeanDynamizer;
import com.teragrep.cfe_39.metrics.mxj.MXJEndpoint;
import com.teragrep.cfe_39.metrics.mxj.MXJItem;

import javax.management.DynamicMBean;
import java.util.ArrayList;
import java.util.List;

public class RuntimeStatistics {
    private final AtomicLongMXJItem threadsStat = new AtomicLongMXJItem("threads");
    private final AtomicLongMXJItem bytesStat = new AtomicLongMXJItem("bytes");
    private final AtomicLongMXJItem recordsStat = new AtomicLongMXJItem("records");


    public DynamicMBean register() {
        List<MXJItem> itemList = new ArrayList<>();
        itemList.add(threadsStat);
        itemList.add(bytesStat);
        itemList.add(recordsStat);

        MXJBeanDynamizer dynamizer = new MXJBeanDynamizer(itemList);

        MXJEndpoint mxjEndpoint = new MXJEndpoint(
                "com.teragrep.cfe_39",
                "Metrics",
                "RuntimeTotals",
                dynamizer.createDynamicMBean()
        );

        return mxjEndpoint.register();
    }

    public long addAndGetThreads(long delta) {
        return threadsStat.getAtomicLong().addAndGet(delta);
    }

    public long addAndGetBytes(long delta) {
        return bytesStat.getAtomicLong().addAndGet(delta);
    }

    public long addAndGetRecords(long delta) {
        return recordsStat.getAtomicLong().addAndGet(delta);
    }
}
