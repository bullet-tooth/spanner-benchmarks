package com.example;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.*;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class SubscriberData {
    public static final String TABLE_NAME = "cao_ldm_00_ent";
    public static final String COLUMN_CC = "cc";
    public static final String COLUMN_MP = "mp";
    public static final List<String> COLUMNS_CONTAINERS = Arrays.asList("fu_01", "fu_02", "fu_03", "fu_04", "su_01", "su_02", "su_03", "su_04");
    public static final String SQL_PREFETCH_PART1 =
            "SELECT mp FROM cao_ldm_00_acc WHERE pk=@pk AND valid_from<=@valid_from ORDER BY valid_from DESC";
    public static final long RECORDS = 100000000L;
    public static final long PREFIX = 491733000000L;
    private final Long cc;
    private final Map<String, ByteArray> data = new HashMap<>();

    private SubscriberData(Long cc) {
        this.cc = cc;
    }

    public Long getCc() {
        return cc;
    }

    public Map<String, ByteArray> getData() {
        return data;
    }

    public static SubscriberData from(Struct row) {
        SubscriberData sd = new SubscriberData(row.getLong(COLUMN_CC));
        for (String container : COLUMNS_CONTAINERS) {
            sd.data.put(container, getBytes(row, container));
        }
        return sd;
    }

    public static Mutation getMutation(SubscriberData sd, String mp) {
        Mutation.WriteBuilder wb = Mutation.newUpdateBuilder(TABLE_NAME)
                .set(COLUMN_MP).to(mp)
                .set(COLUMN_CC).to(sd.getCc());
        for (Map.Entry<String, ByteArray> de : sd.getData().entrySet()) {
            wb.set(de.getKey()).to(de.getValue());
        }
        return wb.build();
    }

    public static Mutation getRandomMutation() {
        long customerId = getRandomCustomer();
        String mp = "30 " + customerId + " ";
        SubscriberData sd = new SubscriberData(ThreadLocalRandom.current().nextLong(RECORDS));
        return getMutation(sd, mp);
    }

    public static String execPrefetch(DatabaseClient client) {
        long customerId = getRandomCustomer();
        // --- Prefetch ---
        Statement statement = Statement.newBuilder(SQL_PREFETCH_PART1)
                .bind("pk").to("169 " + customerId + " 0 ")
                .bind("valid_from").to(System.currentTimeMillis())
                .build();

        ResultSet resultSet = client.singleUse()
                .executeQuery(statement);
        final String mp = resultSet.next() ? resultSet.getString(COLUMN_MP) : null;
        resultSet.close();

        return mp;
    }

    public static Struct readRow(TransactionContext transaction, String mp) {
        return transaction.readRow(TABLE_NAME, Key.of(mp),
                ImmutableList.<String>builder()
                        .add(COLUMN_CC)
                        .addAll(COLUMNS_CONTAINERS)
                        .build());
    }


    private static long getRandomCustomer() {
        return PREFIX + ThreadLocalRandom.current().nextLong(RECORDS);
    }

    private static ByteArray getBytes(Struct row, String column) {
        return row.isNull(column) ? null : row.getBytes(column);
    }
}
