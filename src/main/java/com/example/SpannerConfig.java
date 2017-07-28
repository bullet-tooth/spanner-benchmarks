package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SpannerConfig {
    private static class Holder {
        static final SpannerConfig instance = new SpannerConfig();
    }
    private Properties properties;

    private SpannerConfig() {
        this.properties = new Properties();
        load();
    }

    public static SpannerConfig getConfig() {
        return Holder.instance;
    }

    private void load() {
        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("spanner.properties")) {
            properties.load(inputStream);
        } catch (IOException e) {
            System.err.println("Can't load properties " + e);
        }
    }


    public String getDatabase() {
        return getStringProperty("spanner.database");
    }

    public String getInstance() {
        return getStringProperty("spanner.instance");
    }

    public int minSessions() {
        return Integer.parseInt(getStringProperty("spanner.sessions.min"));
    }
    public int maxSessions() {
        return Integer.parseInt(getStringProperty("spanner.sessions.max"));
    }

    public int channelsNum() {
        return Integer.parseInt(getStringProperty("spanner.channels.num"));
    }

    public float writeSessionFraction() {
        return Float.parseFloat(getStringProperty("spanner.sessions.write.fraction"));
    }

    public int clientThreadsNum() {
        return Integer.parseInt(getStringProperty("spanner.clientthreads.num"));
    }

    private String getStringProperty(String name) {
        return System.getProperty(name) != null ? System.getProperty(name) : properties.getProperty(name);
    }

    public int customersCount() {
        return Integer.parseInt(getStringProperty("spanner.customers.count"));
    }

}
