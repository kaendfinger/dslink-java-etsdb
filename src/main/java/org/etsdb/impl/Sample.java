package org.etsdb.impl;

import java.io.Serializable;
import java.util.Arrays;

public class Sample implements Serializable {
    private static final long serialVersionUID = 1L;

    private int seriesId;
    private long ts;
    private byte[] data;

    public Sample() {
        // no op
    }

    public Sample(int seriesId, long ts, byte[] data) {
        this.seriesId = seriesId;
        this.ts = ts;
        this.data = data.clone();
    }

    public int getSeriesId() {
        return seriesId;
    }

    public void setSeriesId(int seriesId) {
        this.seriesId = seriesId;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public byte[] getData() {
        return data.clone();
    }

    public void setData(byte[] data) {
        this.data = data.clone();
    }

    @Override
    public String toString() {
        return "Sample [seriesId=" + seriesId + ", ts=" + ts + ", data=" + Arrays.toString(data) + "]";
    }
}
