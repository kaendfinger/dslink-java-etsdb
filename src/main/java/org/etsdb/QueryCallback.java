package org.etsdb;

public interface QueryCallback<T> {
    void sample(String seriesId, long ts, T value);
}
