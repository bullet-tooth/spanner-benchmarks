package com.example;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.*;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.example.SubscriberData.*;

public class DBHelper {

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

        return getMutation(generateRandomCustomerData(), getRandomMp());
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

}
