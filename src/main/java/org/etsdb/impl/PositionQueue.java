package org.etsdb.impl;

import org.etsdb.util.queue.LongLimitQueue;
import org.etsdb.util.queue.LongQueue;

/**
 * Remembers the positions of rows in shards to facilitate reverse queries. Limit queues are used to simplify limit
 * queries.
 *
 * @author Matthew
 */
public class PositionQueue {
    private final LongQueue queue;
    private final LongLimitQueue limitQueue;

    public PositionQueue(int limit) {
        if (limit == Integer.MAX_VALUE) {
            queue = new LongQueue();
            limitQueue = null;
        } else {
            queue = null;
            limitQueue = new LongLimitQueue(limit);
        }
    }

    public void push(long l) {
        if (queue != null)
            queue.push(l);
        else
            limitQueue.push(l);
    }

    public int size() {
        if (queue != null)
            return queue.size();
        return limitQueue.size();
    }

    public long peek(int index) {
        if (queue != null)
            return queue.peek(index);
        return limitQueue.peek(index);
    }
}
