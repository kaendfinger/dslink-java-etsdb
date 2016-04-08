package org.etsdb.impl;

import java.util.Arrays;

/**
 * A backdated sample that is unwritten.
 *
 * @author Matthew
 */
class Backdate implements Comparable<Backdate> {
    private final String seriesId;
    private final long shardId;
    private final long offset;
    private final byte[] data;

    Backdate(String seriesId, long shardId, long offset, byte[] data, int off, int len) {
        this.seriesId = seriesId;
        this.shardId = shardId;
        this.offset = offset;
        this.data = Utils.copy(data, off, len);
    }

    String getSeriesId() {
        return seriesId;
    }

    long getShardId() {
        return shardId;
    }

    long getOffset() {
        return offset;
    }

    byte[] getData() {
        return data;
    }

    @Override
    public int compareTo(Backdate that) {
        return Utils.compareLong(offset, that.offset);
    }

    @Override
    @SuppressWarnings("SimplifiableIfStatement")
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Backdate backdate = (Backdate) o;

        if (getShardId() != backdate.getShardId()) return false;
        if (getOffset() != backdate.getOffset()) return false;
        if (getSeriesId() != null ? !getSeriesId().equals(backdate.getSeriesId()) : backdate.getSeriesId() != null)
            return false;
        return Arrays.equals(getData(), backdate.getData());

    }

    @Override
    public int hashCode() {
        int result = getSeriesId() != null ? getSeriesId().hashCode() : 0;
        result = 31 * result + (int) (getShardId() ^ (getShardId() >>> 32));
        result = 31 * result + (int) (getOffset() ^ (getOffset() >>> 32));
        result = 31 * result + (getData() != null ? Arrays.hashCode(getData()) : 0);
        return result;
    }
}
