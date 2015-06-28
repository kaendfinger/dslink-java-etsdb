package org.etsdb.serializer;

import org.etsdb.ByteArrayBuilder;
import org.etsdb.Serializer;

public class StringSerializer extends Serializer<String> {
    private static final StringSerializer instance = new StringSerializer();

    public static StringSerializer get() {
        return instance;
    }

    @Override
    public void toByteArray(ByteArrayBuilder b, String s, long ts) {
        b.putString(s);
    }

    @Override
    public String fromByteArray(ByteArrayBuilder b, long ts) {
        return b.getString();
    }
}
