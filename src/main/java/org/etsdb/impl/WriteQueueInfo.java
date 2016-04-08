package org.etsdb.impl;

import org.dsa.iot.etsdb.utils.atomic.NotifyAtomicInteger;
import org.etsdb.DbConfig;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

class WriteQueueInfo {
    final int discardQueueSize;
    final int expireMinimum;
    final int expireMaximum;
    final int shardQueueSizeMinimum;
    final int shardQueueSizeMaximum;
    final int maxQueueSize;

    final NotifyAtomicInteger queueSize = new NotifyAtomicInteger();
    final AtomicInteger recentDiscards = new AtomicInteger();
    final Random random = new Random();

    public WriteQueueInfo(DbConfig config) {
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
