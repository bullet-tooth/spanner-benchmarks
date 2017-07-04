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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.*;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

import static com.example.SubscriberData.*;

@Fork(1)
@Warmup(iterations = 5, time = 1)
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5)

public class BenchmarkSpanner {
    private static final long TIME_TO_SLEEP = 20L;

    @Benchmark
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


    @Benchmark
    public void prefetch(SpannerConnection connection) {
        DatabaseClient client = connection.getClient();
        execPrefetch(client);
    }


    @Benchmark
    public void callSetup(SpannerConnection connection) {
        DatabaseClient client = connection.getClient();
        final String mp = execPrefetch(client);

        client.readWriteTransaction()
                .run(new TransactionRunner.TransactionCallable<Void>() {
                    @Nullable
                    @Override
                    public Void run(TransactionContext transaction) throws Exception {
                        Struct row = readRow(transaction, mp);
                        Thread.sleep(TIME_TO_SLEEP);
                        if (row != null) {
                            transaction.buffer(getMutation(SubscriberData.from(row), mp));
                        }
                        return null;
                    }
                });
    }


    @Benchmark
    public void blindWrite(SpannerConnection connection) {
        connection.getClient().write(ImmutableList.of(getRandomMutation()));
    }
}
