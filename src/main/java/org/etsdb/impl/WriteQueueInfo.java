package org.etsdb.impl;

import org.dsa.iot.etsdb.utils.atomic.NotifyAtomicInteger;
import org.etsdb.DbConfig;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

class WriteQueueInfo {
    final int maxQueueSize;
    final int discardQueueSize;
    final NotifyAtomicInteger queueSize = new NotifyAtomicInteger();
    final AtomicInteger recentDiscards = new AtomicInteger();
    final Random random = new Random();

    private final int expireMinimum;
    private final int expireMaximum;
    private final int shardQueueSizeMinimum;
    private final int shardQueueSizeMaximum;

    WriteQueueInfo(DbConfig config) {
        expireMinimum = config.getQueueExpireMinimum();
        expireMaximum = config.getQueueExpireMaximum();
        shardQueueSizeMinimum = config.getQueueShardQueueSizeMinimum();
        shardQueueSizeMaximum = config.getQueueShardQueueSizeMaximum();
        maxQueueSize = config.getQueueMaxQueueSize();
        discardQueueSize = config.getQueueDiscardQueueSize();
    }

    public long getExpiryTime() {
        if (expireMinimum == expireMaximum)
            return System.currentTimeMillis() + expireMinimum;
        return System.currentTimeMillis() + expireMinimum + random.nextInt(expireMaximum - expireMinimum);
    }

    public int getShardQueueSize() {
        if (shardQueueSizeMinimum == shardQueueSizeMaximum)
            return shardQueueSizeMinimum;
        return shardQueueSizeMinimum + random.nextInt(shardQueueSizeMaximum - shardQueueSizeMinimum);
    }
}
