package com.example;

import com.google.cloud.spanner.*;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.ExecutionException;

@State(Scope.Benchmark)
public class SpannerConnection {
    private DatabaseId db;
    private DatabaseClient dbClient;
    private Spanner spanner;

    @Setup
    public void init() {
        SpannerConfig config = SpannerConfig.getConfig();

        SpannerOptions options = SpannerOptions.newBuilder()
                .setSessionPoolOption(
                        SessionPoolOptions.newBuilder()
                                .setFailIfPoolExhausted()
                                .setMinSessions(config.minSessions())
                                .setWriteSessionsFraction(config.writeSessionFraction())
                                .build())
                .setNumChannels(config.channelsNum())
                .build();
        spanner = options.getService();
        db = DatabaseId.of(options.getProjectId(), config.getInstance(), config.getDatabase());

        String clientProject = spanner.getOptions().getProjectId();
        if (!db.getInstanceId().getProject().equals(clientProject)) {
            System.err.println("Invalid project specified. Project in the database id should match"
                    + "the project name set in the environment variable GCLOUD_PROJECT. Expected: "
                    + clientProject);
            System.exit(1);
        }
        dbClient = spanner.getDatabaseClient(db);
    }


    public DatabaseClient getClient() {
        return dbClient;
    }


    @TearDown
    public void closeConnection() throws ExecutionException, InterruptedException {
        spanner.closeAsync().get();
        System.out.println("Connection closed");
    }
}
