package org.dsa.iot.etsdb.serializer;

import io.netty.util.CharsetUtil;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.etsdb.ByteArrayBuilder;
import org.etsdb.Serializer;

/**
 * @author Samuel Grenier
 */
public class ValueSerializer extends Serializer<ByteData> {

    public static final byte NUMBER = 0;
    public static final byte BOOL = 1;
    public static final byte STRING = 2;
    public static final byte MAP = 3;
    public static final byte ARRAY = 4;
    public static final byte BINARY = 5;

    public static final byte BYTE = 0;
    public static final byte SHORT = 1;
    public static final byte INT = 2;
    public static final byte LONG = 3;
    public static final byte FLOAT = 4;
    public static final byte DOUBLE = 5;

    @Override public void toByteArray(ByteArrayBuilder b, ByteData data, long ts) {
        if (data == null) {
            return;
        }

        Value val = data.getValue();
        ValueType type = val.getType();
        if (type.compare(ValueType.NUMBER)) {
            b.put(NUMBER);
            Number number = val.getNumber();
            if (number == null) {
                b.put(BYTE);
                b.put((byte) 0);
            } else if (number instanceof Byte) {
                b.put(BYTE);
                b.put(number.byteValue());
            } else if (number instanceof Short) {
                b.put(SHORT);
                b.putShort(number.shortValue());
            } else if (number instanceof Integer) {
                b.put(INT);
                b.putInt(number.intValue());
            } else if (number instanceof Long) {
                b.put(LONG);
                b.putLong(number.longValue());
            } else if (number instanceof Float) {
                b.put(FLOAT);
                b.putFloat(number.floatValue());
            } else {
                b.put(DOUBLE);
                b.putDouble(number.doubleValue());
            }
        } else if (type.compare(ValueType.BOOL)) {
            b.put(BOOL);
            b.putBoolean(val.getBool());
        } else if (type.compare(ValueType.STRING)) {
            b.put(STRING);
            b.putString(val.getString());
        } else if (type.compare(ValueType.MAP)) {
            b.put(MAP);
            b.putString(new String(val.getMap().encode(), CharsetUtil.UTF_8));
        } else if (type.compare(ValueType.ARRAY)) {
            b.put(ARRAY);
            b.putString(new String(val.getArray().encode(), CharsetUtil.UTF_8));
        } else if (type.compare(ValueType.BINARY)) {
            b.put(BINARY);
            b.put(val.getBinary());
        }
    }

    @Override public ByteData fromByteArray(ByteArrayBuilder b, long ts) {
        if (b.getAvailable() <= 0) {
            return null;
        }

        ByteData data = new ByteData();
        data.setType(b.get());
        data.setTimestamp(ts);
        {
            byte[] a = new byte[b.getAvailable()];
            b.get(a, 0, b.getAvailable());
            data.setBytes(a);
        }

        return data;
    }
}
