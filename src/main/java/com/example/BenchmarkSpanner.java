/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.example;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.*;
import org.openjdk.jmh.annotations.*;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;


@Fork(1)
@Warmup(iterations = 5, time = 1)
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5)

public class BenchmarkSpanner {
    private static final String SQL_PREFETCH_PART1 =
            "SELECT mp FROM cao_ldm_00_acc WHERE pk=@pk AND valid_from<=@valid_from ORDER BY valid_from DESC";
    private static final long RECORDS = 100000000L;
    public static final long PREFIX = 491733000000L;

    @Benchmark()
    public void emptyRW(SpannerConnection connection) {
        DatabaseClient client = connection.getClient();
        client.readWriteTransaction()
                .run(new TransactionRunner.TransactionCallable<Void>() {
                    @Nullable
                    @Override
                    public Void run(TransactionContext transaction) throws Exception {
                        return null;
                    }
                });
    }


    @Benchmark()
    public void prefetch(SpannerConnection connection) {
        DatabaseClient client = connection.getClient();
        long customerId = getRandomCustomer();

        // --- Prefetch ---
        Statement statement = Statement.newBuilder(SQL_PREFETCH_PART1)
                .bind("pk").to("169 " + customerId + " 0 ")
                .bind("valid_from").to(System.currentTimeMillis())
                .build();

        ResultSet resultSet = client.singleUse()
                .executeQuery(statement);
        final String mp = resultSet.next() ? resultSet.getString("mp") : null;
        resultSet.close();
    }


    @Benchmark()
    public void callSetup(SpannerConnection connection) {
        DatabaseClient client = connection.getClient();
        long customerId = getRandomCustomer();

        // --- Prefetch ---
        Statement statement = Statement.newBuilder(SQL_PREFETCH_PART1)
                .bind("pk").to("169 " + customerId + " 0 ")
                .bind("valid_from").to(System.currentTimeMillis())
                .build();

        ResultSet resultSet = client.singleUse()
                .executeQuery(statement);
        final String mp = resultSet.next() ? resultSet.getString("mp") : null;
        resultSet.close();


        client.readWriteTransaction()
                .run(new TransactionRunner.TransactionCallable<Void>() {
                    @Nullable
                    @Override
                    public Void run(TransactionContext transaction) throws Exception {
                        Struct row = transaction.readRow("cao_ldm_00_ent", Key.of(mp),
                                Arrays.asList("cc", "fu_01", "fu_02", "fu_03", "fu_04", "su_01", "su_02", "su_03", "su_04"));

                        Thread.sleep(20);

                        if (row != null) {
                            long cc = row.getLong("cc");
                            ByteArray fu_01 = row.isNull("fu_01") ? null : row.getBytes("fu_01");
                            ByteArray fu_02 = row.isNull("fu_02") ? null : row.getBytes("fu_02");
                            ByteArray fu_03 = row.isNull("fu_03") ? null : row.getBytes("fu_03");
                            ByteArray fu_04 = row.isNull("fu_04") ? null : row.getBytes("fu_04");
                            ByteArray su_01 = row.isNull("su_01") ? null : row.getBytes("su_01");
                            ByteArray su_02 = row.isNull("su_02") ? null : row.getBytes("su_02");
                            ByteArray su_03 = row.isNull("su_03") ? null : row.getBytes("su_03");
                            ByteArray su_04 = row.isNull("su_04") ? null : row.getBytes("su_04");

                            transaction.buffer(Mutation.newUpdateBuilder("cao_ldm_00_ent")
                                    .set("mp").to(mp)
                                    .set("cc").to(cc)
                                    .set("fu_01").to(fu_01)
                                    .set("fu_02").to(fu_02)
                                    .set("fu_03").to(fu_03)
                                    .set("fu_04").to(fu_04)
                                    .set("su_01").to(su_01)
                                    .set("su_02").to(su_02)
                                    .set("su_03").to(su_03)
                                    .set("su_04").to(su_04)
                                    .build()
                            );
                        }
                        return null;
                    }
                });
    }


    @Benchmark
    public void blindWrite(SpannerConnection connection) {
        long customerId = getRandomCustomer();
        String mp = "30 " + customerId + " ";

        Mutation mutation = Mutation.newUpdateBuilder("cao_ldm_00_ent")
                .set("mp").to(mp)
                .set("cc").to(0)
                .build();

        connection.getClient().write(Collections.singletonList(mutation));
    }


    private long getRandomCustomer() {
        return PREFIX + (long) (RECORDS * Math.random());
    }
}
