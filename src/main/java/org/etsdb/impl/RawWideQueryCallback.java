package org.etsdb.impl;

import org.etsdb.ByteArrayBuilder;

public interface RawWideQueryCallback {
    void preQuery(String seriesId, long ts, ByteArrayBuilder b);

    void sample(String seriesId, long ts, ByteArrayBuilder b);

    void postQuery(String seriesId, long ts, ByteArrayBuilder b);
}
