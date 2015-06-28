package org.etsdb;

public interface WideQueryCallback<T> {
    void preQuery(String seriesId, long ts, T value);

    void sample(String seriesId, long ts, T value);

    void postQuery(String seriesId, long ts, T value);
}
