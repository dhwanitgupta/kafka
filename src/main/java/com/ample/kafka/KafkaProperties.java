package com.ample.kafka;

import java.util.Properties;

/**
 * Created by Dhwanit on 13/05/16.
 */
public class KafkaProperties {

    public final static String BOOTSTRAP_SERVER = "localhost:9092";
    public final static String GROUP_ID = "test";
    public final static String ENABLE_AUTO_COMMIT = "true";
    public final static String AUTO_COMMIT_INTERVAL_MS = "1000";
    public final static String SESSION_TIMEOUT_MS = "30000";
    public final static String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public final static String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    public Properties properties;

    public KafkaProperties() {

        properties = new Properties();

        properties.put("bootstrap.servers", BOOTSTRAP_SERVER);
        properties.put("group.id", GROUP_ID);
        properties.put("enable.auto.commit", ENABLE_AUTO_COMMIT);
        properties.put("auto.commit.interval.ms", AUTO_COMMIT_INTERVAL_MS);
        properties.put("session.timeout.ms", SESSION_TIMEOUT_MS);
        properties.put("key.deserializer", KEY_DESERIALIZER);
        properties.put("value.deserializer", VALUE_DESERIALIZER);
    }

    public Properties getProperties() {
        return this.properties;
    }
}
