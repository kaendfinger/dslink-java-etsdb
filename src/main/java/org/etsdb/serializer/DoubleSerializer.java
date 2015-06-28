package org.etsdb.serializer;

import org.etsdb.ByteArrayBuilder;
import org.etsdb.Serializer;

public class DoubleSerializer extends Serializer<Double> {
    private static final DoubleSerializer instance = new DoubleSerializer();

    public static DoubleSerializer get() {
        return instance;
    }

    @Override
    public void toByteArray(ByteArrayBuilder b, Double d, long ts) {
        b.putDouble(d);
    }

    @Override
    public Double fromByteArray(ByteArrayBuilder b, long ts) {
        return b.getDouble();
    }
}
