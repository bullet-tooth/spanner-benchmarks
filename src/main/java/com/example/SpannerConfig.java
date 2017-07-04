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
        return properties.getProperty("spanner.database");
    }

    public String getInstance() {
        return properties.getProperty("spanner.instance");
    }

    public int minSessions() {
        return Integer.parseInt(System.getProperty("spanner.sessions.min", "100"));
    }

    public int channelsNum() {
        return Integer.parseInt(System.getProperty("spanner.channels.num", "16"));
    }

    public float writeSessionFraction() {
        return Float.parseFloat(System.getProperty("spanner.sessions.write.fraction", "0f"));
    }

    public int clientThreadsNum() {
        return Integer.parseInt(System.getProperty("spanner.clientthreads.num", "100"));
    }
}
