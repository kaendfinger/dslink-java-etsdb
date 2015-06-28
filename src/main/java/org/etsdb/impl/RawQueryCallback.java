package org.etsdb.impl;

import org.etsdb.ByteArrayBuilder;

public interface RawQueryCallback {
    void sample(String seriesId, long ts, ByteArrayBuilder b);
}
