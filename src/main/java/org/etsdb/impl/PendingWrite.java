package org.etsdb.impl;

import java.util.Arrays;

public class PendingWrite implements Comparable<PendingWrite> {
    private final long offset;
    private final byte[] data;

    public PendingWrite(long offset, byte[] data, int off, int len) {
        this(offset, Utils.copy(data, off, len));
    }

    public PendingWrite(long offset, byte[] data) {
        this.offset = offset;
        this.data = data != null ? data.clone() : null;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    @SuppressWarnings("SimplifiableIfStatement")
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PendingWrite that = (PendingWrite) o;
        if (getOffset() != that.getOffset()) return false;
        return Arrays.equals(data, that.data);

    }

    @Override
    public int hashCode() {
        int result = (int) (getOffset() ^ (getOffset() >>> 32));
        result = 31 * result + (data != null ? Arrays.hashCode(data) : 0);
        return result;
    }

    public byte[] getData() {
        return data != null ? data.clone() : null;
    }

    @Override
    public int compareTo(PendingWrite that) {
        return Utils.compareLong(offset, that.offset);
    }
}
