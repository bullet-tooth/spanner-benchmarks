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

    public boolean indexesEnabled() {
        return Boolean.parseBoolean(properties.getProperty("spanner.indexes.enabled"));
    }

    public boolean recreateSchema() {
        return Boolean.parseBoolean(properties.getProperty("spanner.schema.recreate"));
    }

    public boolean loadData() {
        return Boolean.parseBoolean(properties.getProperty("spanner.data.load"));
    }

    public boolean loadDataRef() {
        return Boolean.parseBoolean(properties.getProperty("spanner.data.ref.load"));
    }

    public boolean flatSchema() {
        return Boolean.parseBoolean(properties.getProperty("spanner.flat.schema"));
    }

    public int batchSizeEnt() {
        return Integer.parseInt(properties.getProperty("spanner.batch.ent"));
    }

    public int batchSizeAcc() {
        return Integer.parseInt(properties.getProperty("spanner.batch.acc"));
    }

    public int batchSizeFus() {
        return Integer.parseInt(properties.getProperty("spanner.batch.fus"));
    }

    public int batchSizeSus() {
        return Integer.parseInt(properties.getProperty("spanner.batch.sus"));
    }

    public int batchSizeBal() {
        return Integer.parseInt(properties.getProperty("spanner.batch.bal"));
    }

    public int dataSplits() {
        return Integer.parseInt(properties.getProperty("spanner.data.splits"));
    }
    public int refSize() {
        return Integer.parseInt(properties.getProperty("spanner.data.ref.size"));
    }
    public int refDepth() {
        return Integer.parseInt(properties.getProperty("spanner.data.ref.depth.max"));
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
