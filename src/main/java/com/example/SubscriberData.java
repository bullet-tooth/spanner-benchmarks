package com.example;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;

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
    public static final long RECORDS = 99999900L;
    public static final long PREFIX = 491733000000L;
    private final Long cc;
    private final Map<String, ByteArray> data = new HashMap<>();

     SubscriberData(Long cc) {
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


    public static long getRandomCustomer() {
        return PREFIX + ThreadLocalRandom.current().nextLong(RECORDS);
    }

    private static ByteArray getBytes(Struct row, String column) {
        return row.isNull(column) ? null : row.getBytes(column);
    }
}
