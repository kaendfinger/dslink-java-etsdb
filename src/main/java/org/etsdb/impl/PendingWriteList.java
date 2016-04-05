package org.etsdb.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class PendingWriteList {
    private final WriteQueueInfo queueInfo;
    private final List<PendingWrite> list = new ArrayList<>();
    private long expiryTime;
    private int maxSize;

    PendingWriteList(WriteQueueInfo queueInfo) {
        this.queueInfo = queueInfo;
    }

    boolean expired(long runtime) {
        return expiryTime != 0 && expiryTime <= runtime;
    }

    void add(PendingWrite sample) {
        if (list.isEmpty()) {
            expiryTime = queueInfo.getExpiryTime();
            maxSize = queueInfo.getShardQueueSize();
            list.add(sample);
        } else {
            // There is a possibility that this write is backdated compared to the list or is an overwrite, so do a 
            // search to find its insert position.
            int index = Collections.binarySearch(list, sample);
            if (index < 0) {
                index = -index - 1;
                if (index == list.size()) {
                    list.add(sample);
                } else {
                    list.add(index, sample);
                }
            } else {
                list.set(index, sample);
            }
        }
    }

    void clear() {
        if (!list.isEmpty()) {
            list.clear();
            expiryTime = 0;
        }
    }

    boolean exceeds() {
        return list.size() > maxSize;
    }

    boolean isEmpty() {
        return list.isEmpty();
    }

    List<PendingWrite> getList() {
        return list;
    }
}
