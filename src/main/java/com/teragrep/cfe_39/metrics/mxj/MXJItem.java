package com.teragrep.cfe_39.metrics.mxj;

import java.util.function.Consumer;
import java.util.function.Supplier;

public interface MXJItem<T> {
    Class<T> getAttributeType();

    String getName();

    Supplier<T> getReader();

    Consumer<T> getSetter();
}