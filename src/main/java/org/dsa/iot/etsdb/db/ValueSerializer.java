package org.dsa.iot.etsdb.db;

import io.netty.util.CharsetUtil;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.json.JsonArray;
import org.dsa.iot.dslink.util.json.JsonObject;
import org.etsdb.ByteArrayBuilder;
import org.etsdb.Serializer;

/**
 * @author Samuel Grenier
 */
public class ValueSerializer extends Serializer<Value> {

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

    @Override
    public void toByteArray(ByteArrayBuilder b, Value val, long ts) {
        if (val == null) {
            return;
        }

        ValueType type = val.getType();
        if (type.compare(ValueType.NUMBER)) {
            b.put(NUMBER);
            Number number = val.getNumber();
            if (number instanceof Byte) {
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

    @Override
    public Value fromByteArray(ByteArrayBuilder b, long ts) {
        if (b.getAvailable() <= 0) {
            return null;
        }

        Value value;
        int type = b.get();
        switch (type) {
            case NUMBER: {
                type = b.get();
                switch (type) {
                    case BYTE:
                        value = new Value(b.get());
                        break;
                    case SHORT:
                        value = new Value(b.getShort());
                        break;
                    case INT:
                        value = new Value(b.getInt());
                        break;
                    case LONG:
                        value = new Value(b.getLong());
                        break;
                    case FLOAT:
                        value = new Value(b.getFloat());
                        break;
                    case DOUBLE:
                        value = new Value(b.getDouble());
                        break;
                    default:
                        throw new RuntimeException("Unsupported type: " + type);
                }
                break;
            }
            case BOOL: {
                value = new Value(b.getBoolean());
                break;
            }
            case STRING: {
                value = new Value(b.getString());
                break;
            }
            case MAP: {
                JsonObject obj = new JsonObject(b.getString());
                value = new Value(obj);
                break;
            }
            case ARRAY: {
                JsonArray array = new JsonArray(b.getString());
                value = new Value(array);
                break;
            }
            case BINARY: {
                int avail = b.getAvailable();
                byte[] bytes = new byte[avail];
                b.get(bytes, 0, avail);
                value = new Value(bytes);
                break;
            }
            default:
                throw new RuntimeException("Unsupported type: " + type);
        }

        value.setTime(ts);
        return value;
    }
}
