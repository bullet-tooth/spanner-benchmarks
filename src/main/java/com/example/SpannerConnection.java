package com.example;

import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.*;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@State(Scope.Benchmark)
public class SpannerConnection {
    private DatabaseClient dbClient;
    private Spanner spanner;
    private volatile boolean connectionClosed;

    private static class SpannerExecutorFactory implements
            GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService> {
        ScheduledThreadPoolExecutor service;

        SpannerExecutorFactory(int numThreads) {
            service = new ScheduledThreadPoolExecutor(numThreads);
        }

        @Override
        public ScheduledExecutorService get() {
            return service;
        }

        @Override
        public void release(ScheduledExecutorService service) {
            service.shutdown();
        }
    }

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
                .setTransportOptions(GrpcTransportOptions.newBuilder()
                        .setExecutorFactory(new SpannerExecutorFactory(config.clientThreadsNum()))
                        .build())
                .build();
        spanner = options.getService();
        DatabaseId db = DatabaseId.of(options.getProjectId(), config.getInstance(), config.getDatabase());

        String clientProject = spanner.getOptions().getProjectId();
        if (!db.getInstanceId().getProject().equals(clientProject)) {
            System.err.println("Invalid project specified. Project in the database id should match"
                    + "the project name set in the environment variable GCLOUD_PROJECT. Expected: "
                    + clientProject);
            System.exit(1);
        }
        dbClient = spanner.getDatabaseClient(db);

        Runtime.getRuntime().addShutdownHook(new Thread("spannerShutdown") {
            @Override
            public void run() {
                closeConnection();
            }
        });
    }


    public DatabaseClient getClient() {
        return dbClient;
    }


    @TearDown
    public synchronized void closeConnection() {
        if (!connectionClosed) {
            spanner.close();
            System.out.println("Spanner connection closed");
        }
        connectionClosed = true;
    }
}
