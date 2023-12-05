package com.teragrep.cfe_39.metrics.mxj;

import javax.management.*;
import java.lang.management.ManagementFactory;

public class MXJEndpoint {

    private final String domain;
    private final String type;
    private final String name;
    private final DynamicMBean dynamicMBean;

    public MXJEndpoint(String domain, String type, String name, DynamicMBean dynamicMBean) {
        this.domain = domain;
        this.type = type;
        this.name = name;
        this.dynamicMBean = dynamicMBean;
    }

    public DynamicMBean register() {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        try {
            ObjectName objectName = new ObjectName(
                    domain + ":type=" + type + ",name=" + name
            );

            mBeanServer.registerMBean(
                    dynamicMBean,
                    objectName
            );
            return dynamicMBean;
        } catch (MalformedObjectNameException
                 | NotCompliantMBeanException
                 | InstanceAlreadyExistsException
                 | MBeanRegistrationException exception) {
            throw new RuntimeException(exception);
        }
    }
}