package org.etsdb.serializer;

import org.etsdb.ByteArrayBuilder;
import org.etsdb.Serializer;

public class ByteArraySerializer extends Serializer<byte[]> {
    private static final ByteArraySerializer instance = new ByteArraySerializer();

    public static ByteArraySerializer get() {
        return instance;
    }

    @Override
    public void toByteArray(ByteArrayBuilder b, byte[] a, long ts) {
        b.put(a);
    }

    @Override
    public byte[] fromByteArray(ByteArrayBuilder b, long ts) {
        byte[] a = new byte[b.getAvailable()];
        b.get(a, 0, b.getAvailable());
        return a;
    }
}
