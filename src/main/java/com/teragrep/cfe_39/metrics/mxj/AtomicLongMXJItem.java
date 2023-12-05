package com.teragrep.cfe_39.metrics.mxj;


import com.teragrep.cfe_39.metrics.mxj.MXJItem;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AtomicLongMXJItem implements MXJItem<Long> {
    private final Class<Long> attributeType;
    private final String name;
    private final Supplier<Long> reader;
    private final Consumer<Long> setter;

    private final AtomicLong atomicLong = new AtomicLong();
    public AtomicLongMXJItem(String name) {
        this.attributeType = Long.class;
        this.name = name;
        this.reader = atomicLong::get;
        this.setter = atomicLong::set;
    }

    public Class<Long> getAttributeType() {
        return attributeType;
    }

    public String getName() {
        return name;
    }

    public Supplier<Long> getReader() {
        return reader;
    }

    public Consumer<Long> getSetter() {
        return setter;
    }

    public AtomicLong getAtomicLong() {
        return atomicLong;
    }
}
