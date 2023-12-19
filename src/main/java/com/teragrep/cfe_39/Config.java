package com.teragrep.cfe_39;

import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;

public class Config {

    // db
    private final String dbConnectionUrl;
    private final String dbUsername;
    private final String dbPassword;

    // kafka
    private final String queueTopicPattern;

    private final Properties kafkaConsumerProperties;

    private final boolean replicationEnabled;
    private final int streamSize;
    private final String streamUnits;
    private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);
    private final int dropPartitionsOlderThanHours;
    private final int createPartitionsInAdvanceHours;
    private final boolean overrideTableLocation;
    private final String tableLocation;
    private final String hdfsPath;
    private final String hdfsuri;
    private final String queueDirectory;
    private final String queueNamePrefix;

    Config() throws IOException {
        Properties properties = new Properties();
        Path configPath = Paths.get(System.getProperty("cfe_30.config.location", System.getProperty("user.dir") + "/etc/application.properties"));
        LOGGER.info("Loading application config '" + configPath.toAbsolutePath() + "'");
        properties.load(Files.newInputStream(configPath));
        LOGGER.debug("Got configuration: " + properties);

        // db
        this.dbConnectionUrl = properties.getProperty("db.connectionUrl");
        if (this.dbConnectionUrl == null) {
            throw new IllegalArgumentException("db.connectionUrl not set");
        }
        this.dbUsername = properties.getProperty("db.username");
        if (this.dbUsername == null) {
            throw new IllegalArgumentException("db.username not set");
        }
        this.dbPassword = properties.getProperty("db.password");
        if (this.dbPassword == null) {
            throw new IllegalArgumentException("db.password not set");
        }

        String replicationEnabledString = properties.getProperty("db.replicationEnabled", "false");
        this.replicationEnabled = Boolean.parseBoolean(replicationEnabledString);

        String streamBytesString = properties.getProperty("db.streamSize", "512");
        this.streamSize = Integer.parseInt(streamBytesString);
        this.streamUnits = properties.getProperty("db.streamUnits", "rows");
        this.dropPartitionsOlderThanHours = Integer.parseInt(properties.getProperty("db.dropPartitionsOlderThanHours", "4"));
        if(dropPartitionsOlderThanHours <= 0) {
            throw new IllegalArgumentException("db.dropPartitionsOlderThanHours must be set to >0, got " + dropPartitionsOlderThanHours);
        }
        this.createPartitionsInAdvanceHours = Integer.parseInt(properties.getProperty("db.createPartitionsInAdvanceHours", "8"));
        if(createPartitionsInAdvanceHours <= 0) {
            throw new IllegalArgumentException("createPartitionsInAdvanceHours must be set to >0, got " + createPartitionsInAdvanceHours);
        }

        String overrideTableLocationString = properties.getProperty("db.overrideTableLocation", "false");
        this.overrideTableLocation = Boolean.parseBoolean(overrideTableLocationString);
        this.tableLocation = properties.getProperty("db.tableLocation");
        if(overrideTableLocation && tableLocation.isEmpty()) {
            throw new IllegalArgumentException("db.tableLocation resulted in empty string when db.overrideTableLocation was true");
        }

        // HDFS
        this.hdfsPath = properties.getProperty("hdfsPath", "hdfs:///opt/teragrep/cfe_39/srv/");
        this.hdfsuri = properties.getProperty("hdfsuri", "");

        // AVRO
        this.queueDirectory = properties.getProperty("queueDirectory", "");
        this.queueNamePrefix = properties.getProperty("queueNamePrefix", "");


        // kafka
        this.queueTopicPattern = properties.getProperty("queueTopicPattern", "^.*$");

        this.kafkaConsumerProperties = loadSubProperties(properties, "consumer.");
        String loginConfig = properties.getProperty("java.security.auth.login.config");
        if(loginConfig == null) {
            throw new IOException("Property java.security.auth.login.config does not exist");
        }
        if(!(new File(loginConfig)).isFile()) {
            throw new IOException("File '" + loginConfig + "' set by java.security.auth.login.config does not exist");
        }
        System.setProperty("java.security.auth.login.config", loginConfig);

        // Just for loggers to work
        Path log4j2Config = Paths.get(properties.getProperty("log4j2.configurationFile", System.getProperty("user.dir") + "/etc/log4j2.properties"));
        LOGGER.info("Loading log4j2 config from '" + log4j2Config.toRealPath() + "'");
        Configurator.reconfigure(log4j2Config.toUri());
    }

    private Properties loadSubProperties(Properties properties, String prefix) {
        Properties subProperties = new Properties();

        Enumeration<Object> keys = properties.keys();
        while (keys.hasMoreElements()) {
            String key = String.valueOf(keys.nextElement());
            if (key.startsWith(prefix)) {
                String value = properties.getProperty(key);
                String subKey = key.replaceFirst(prefix,"");
                subProperties.put(subKey, value);
            }
        }
        return subProperties;
    }

    public boolean isOverrideTableLocation() {
        return overrideTableLocation;
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public String getDbConnectionUrl() {
        return dbConnectionUrl;
    }

    public String getDbUsername() {
        return dbUsername;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }
    public String getHdfsuri() {
        return hdfsuri;
    }

    public String getQueueDirectory() {
        return queueDirectory;
    }
    public String getQueueNamePrefix() {
        return queueNamePrefix;
    }

    public boolean isReplicationEnabled() {
        return replicationEnabled;
    }

    public int getStreamSize() {
        return streamSize;
    }

    public String getStreamUnits() {
        return streamUnits;
    }

    public String getQueueTopicPattern() {
        return queueTopicPattern;
    }

    public Properties getKafkaConsumerProperties() {
        return kafkaConsumerProperties;
    }

    public int getDropPartitionsOlderThanHours() {
        return dropPartitionsOlderThanHours;
    }

    public int getCreatePartitionsInAdvanceHours() {
        return createPartitionsInAdvanceHours;
    }
}