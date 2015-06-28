package org.etsdb.util.queue;

public class LongLimitQueue {
    private final long[] queue;
    private int head = -1;
    private int tail = 0;
    private int size = 0;

    public LongLimitQueue(int limit) {
        this.queue = new long[limit];
    }

    public void push(long l) {
        this.queue[this.tail] = l;

        this.tail = ((this.tail + 1) % this.queue.length);
        if (this.head == -1) {
            this.head = 0;
            this.size = 1;
        } else if (this.size == this.queue.length) {
            this.head = this.tail;
        } else {
            this.size += 1;
        }
    }

    public long pop() {
        long retval = this.queue[this.head];
        if (this.size == 1) {
            this.head = -1;
            this.tail = 0;
        } else {
            this.head = ((this.head + 1) % this.queue.length);
        }
        this.size -= 1;

        return retval;
    }

    public long peek(int index) {
        if (index >= this.size) {
            throw new IllegalArgumentException("index " + index + " is >= queue size " + this.size);
        }
        index = (index + this.head) % this.queue.length;
        return this.queue[index];
    }

    public int size() {
        return this.size;
    }

    public void clear() {
        this.head = -1;
        this.tail = 0;
        this.size = 0;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        if (this.queue.length == 0) {
            sb.append("[]");
        } else {
            sb.append('[');
            sb.append(this.queue[0]);
            for (int i = 1; i < this.queue.length; i++) {
                sb.append(", ");
                sb.append(this.queue[i]);
            }
            sb.append("]");
        }
        sb.append(", h=").append(this.head).append(", t=").append(this.tail).append(", s=").append(this.size);
        return sb.toString();
    }
}
