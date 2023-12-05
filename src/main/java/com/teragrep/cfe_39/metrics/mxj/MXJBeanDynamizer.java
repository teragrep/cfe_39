package com.teragrep.cfe_39.metrics.mxj;

import com.teragrep.mxj_01.DynamicBean; // TODO: Fix the dependency failing to load.

import javax.management.DynamicMBean;
import java.util.List;

public class MXJBeanDynamizer {
    private final List<MXJItem> mxjItems;

    public MXJBeanDynamizer(List<MXJItem> mxjItems) {
        this.mxjItems = mxjItems;
    }

    public DynamicMBean createDynamicMBean() {
        DynamicBean.Builder builder =  DynamicBean.builder();

        for (MXJItem a : mxjItems) {
            builder = builder.withSimpleAttribute(
                    a.getAttributeType(),
                    a.getName(),
                    a.getReader(),
                    a.getSetter()
            );
        }

        return builder.build();
    }
}
