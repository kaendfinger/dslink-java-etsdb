package org.etsdb.impl;

import org.etsdb.ByteArrayBuilder;

import java.util.List;

/**
 * Reusable object for efficiently handling queries.
 *
 * @author Matthew
 */
class ScanInfo {
    /**
     * The time offset of the current record
     */
    private long offset;
    /**
     * The data for the current record
     */
    private ByteArrayBuilder data = new ByteArrayBuilder(1024);
    /**
     * The pointer to the next cache record.
     */
    private int cacheIndex;
    /**
     * The shard's pending write cache.
     */
    private List<PendingWrite> cache;
    /**
     * If true, the end of file was reached, and scan should be ended. The offset and data fields should not be used.
     */
    private boolean eof;

    public ScanInfo() {
        // no op
    }

    public ScanInfo(List<PendingWrite> cache) {
        this.cache = cache;
        cacheIndex = -1;
    }

    long getOffset() {
        return offset;
    }

    void setOffset(long offset) {
        this.offset = offset;
    }

    ByteArrayBuilder getData() {
        return data;
    }

    void setData(ByteArrayBuilder data) {
        this.data = data;
    }

    boolean isEndOfShard() {
        if (eof && cache != null)
            return cacheIndex >= cache.size();
        return eof;
    }

    boolean isEof() {
        return eof;
    }

    void setEof(boolean eof) {
        this.eof = eof;
        incrementCache();
    }

    void incrementCache() {
        if (cache != null && ++cacheIndex < cache.size()) {
            PendingWrite p = cache.get(cacheIndex);
            offset = p.getOffset();
            data.clear();
            data.put(p.getData());
        }
    }

    void reset() {
        reset(null);
    }

    void reset(DataShard shard) {
        this.eof = false;
        cacheIndex = -1;
        if (shard != null)
            cache = shard.getCache();
    }
}
