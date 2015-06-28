package org.etsdb.util.queue;

public class LongQueue implements Cloneable {
    private long[] queue;
    private int head = -1;
    private int tail = 0;
    private int size = 0;

    public LongQueue() {
        this(128);
    }

    public LongQueue(int initialLength) {
        this.queue = new long[initialLength];
    }

    public LongQueue(long[] i) {
        this(i.length);
        push(i, 0, i.length);
    }

    public LongQueue(long[] i, int pos, int length) {
        this(length);
        push(i, pos, length);
    }

    public void push(long i) {
        if (room() == 0) {
            expand();
        }
        this.queue[this.tail] = i;
        if (this.head == -1) {
            this.head = 0;
        }
        this.tail = ((this.tail + 1) % this.queue.length);
        this.size += 1;
    }

    public void push(long[] i) {
        push(i, 0, i.length);
    }

    public void push(long[] i, int pos, int length) {
        if (length == 0) {
            return;
        }
        while (room() < length) {
            expand();
        }
        int tailLength = this.queue.length - this.tail;
        if (tailLength > length) {
            System.arraycopy(i, pos, this.queue, this.tail, length);
        } else {
            System.arraycopy(i, pos, this.queue, this.tail, tailLength);
        }
        if (length > tailLength) {
            System.arraycopy(i, tailLength + pos, this.queue, 0, length - tailLength);
        }
        if (this.head == -1) {
            this.head = 0;
        }
        this.tail = ((this.tail + length) % this.queue.length);
        this.size += length;
    }

    public void push(LongQueue source) {
        if (source.size == 0) {
            return;
        }
        if (source == this) {
            source = (LongQueue) clone();
        }
        int firstCopyLen = source.queue.length - source.head;
        if (source.size < firstCopyLen) {
            firstCopyLen = source.size;
        }
        push(source.queue, source.head, firstCopyLen);
        if (firstCopyLen < source.size) {
            push(source.queue, 0, source.tail);
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

    public int pop(long[] buf) {
        return pop(buf, 0, buf.length);
    }

    public int pop(long[] buf, int pos, int length) {
        length = peek(buf, pos, length);

        this.size -= length;
        if (this.size == 0) {
            this.head = -1;
            this.tail = 0;
        } else {
            this.head = ((this.head + length) % this.queue.length);
        }
        return length;
    }

    public int pop(int length) {
        if (length == 0) {
            return 0;
        }
        if (this.size == 0) {
            throw new ArrayIndexOutOfBoundsException(-1);
        }
        if (length > this.size) {
            length = this.size;
        }
        this.size -= length;
        if (this.size == 0) {
            this.head = -1;
            this.tail = 0;
        } else {
            this.head = ((this.head + length) % this.queue.length);
        }
        return length;
    }

    public long[] popAll() {
        long[] data = new long[this.size];
        pop(data);
        return data;
    }

    public long tailPop() {
        if (this.size == 0) {
            throw new ArrayIndexOutOfBoundsException(-1);
        }
        this.tail = ((this.tail + this.queue.length - 1) % this.queue.length);
        long retval = this.queue[this.tail];
        if (this.size == 1) {
            this.head = -1;
            this.tail = 0;
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

    public long[] peek(int index, int length) {
        long[] result = new long[length];
        for (int i = 0; i < length; i++) {
            result[i] = peek(index + i);
        }
        return result;
    }

    public int peek(long[] buf) {
        return peek(buf, 0, buf.length);
    }

    public int peek(long[] buf, int pos, int length) {
        if (length == 0) {
            return 0;
        }
        if (this.size == 0) {
            throw new ArrayIndexOutOfBoundsException(-1);
        }
        if (length > this.size) {
            length = this.size;
        }
        int firstCopyLen = this.queue.length - this.head;
        if (length < firstCopyLen) {
            firstCopyLen = length;
        }
        System.arraycopy(this.queue, this.head, buf, pos, firstCopyLen);
        if (firstCopyLen < length) {
            System.arraycopy(this.queue, 0, buf, pos + firstCopyLen, length - firstCopyLen);
        }
        return length;
    }

    public int indexOf(long i) {
        return indexOf(i, 0);
    }

    public int indexOf(long value, int start) {
        if (start >= this.size) {
            return -1;
        }
        int index = (this.head + start) % this.queue.length;
        for (int i = start; i < this.size; i++) {
            if (this.queue[index] == value) {
                return i;
            }
            index = (index + 1) % this.queue.length;
        }
        return -1;
    }

    public int indexOf(long[] values) {
        return indexOf(values, 0);
    }

    public int indexOf(long[] values, int start) {
        if ((values == null) || (values.length == 0)) {
            throw new IllegalArgumentException("cannot search for empty values");
        }
        while (((start = indexOf(values[0], start)) != -1) && (start < this.size - values.length + 1)) {
            boolean found = true;
            for (int i = 1; i < values.length; i++) {
                if (peek(start + i) != values[i]) {
                    found = false;
                    break;
                }
            }
            if (found) {
                return start;
            }
            start++;
        }
        return -1;
    }

    public int size() {
        return this.size;
    }

    public void clear() {
        this.size = 0;
        this.head = -1;
        this.tail = 0;
    }

    private int room() {
        return this.queue.length - this.size;
    }

    private void expand() {
        long[] newq = new long[this.queue.length * 2];
        if (this.head == -1) {
            this.queue = newq;
            return;
        }
        if (this.tail > this.head) {
            System.arraycopy(this.queue, this.head, newq, this.head, this.tail - this.head);
            this.queue = newq;
            return;
        }
        System.arraycopy(this.queue, this.head, newq, this.head + this.queue.length, this.queue.length - this.head);
        System.arraycopy(this.queue, 0, newq, 0, this.tail);
        this.head += this.queue.length;
        this.queue = newq;
    }

    public Object clone() {
        try {
            LongQueue clone = (LongQueue) super.clone();

            clone.queue = this.queue.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
        }
        return null;
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
