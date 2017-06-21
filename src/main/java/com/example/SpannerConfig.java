package com.example;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SpannerConfig {

    private static SpannerConfig instance;

    private Properties properties;

    private SpannerConfig() {
        this.properties = new Properties();
        load();
    }

    public static SpannerConfig getConfig() {
        if (instance == null) {
            synchronized (SpannerConfig.class) {
                if (instance == null) {
                    instance = new SpannerConfig();
                }
            }
        }
        return instance;
    }

    private void load() {
        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("spanner.properties")) {
            properties.load(inputStream);
        } catch (IOException e) {
            System.err.println("Can't load properties " + e);
        }
    }


    public String getDatabase() {
        return properties.getProperty("spanner.database");
    }

    public String getInstance() {
        return properties.getProperty("spanner.instance");
    }

    public int minSessions() {
        return Integer.parseInt(properties.getProperty("spanner.sessions.min"));
    }

    public int channelsNum() {
        return Integer.parseInt(properties.getProperty("spanner.channels.num"));
    }

    public float writeSessionFraction() {
        return Float.parseFloat(properties.getProperty("spanner.sessions.write.fraction"));
    }
}
