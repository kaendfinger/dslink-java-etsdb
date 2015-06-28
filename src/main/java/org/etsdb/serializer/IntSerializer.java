package org.etsdb.serializer;

import org.etsdb.ByteArrayBuilder;
import org.etsdb.Serializer;

public class IntSerializer extends Serializer<Integer> {
    private static final IntSerializer instance = new IntSerializer();

    public static IntSerializer get() {
        return instance;
    }

    @Override
    public void toByteArray(ByteArrayBuilder b, Integer i, long ts) {
        b.putInt(i);
    }

    @Override
    public Integer fromByteArray(ByteArrayBuilder b, long ts) {
        return b.getInt();
    }
}
