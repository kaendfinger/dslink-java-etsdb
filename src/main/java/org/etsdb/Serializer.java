package org.etsdb;

abstract public class Serializer<T> {
    abstract public void toByteArray(ByteArrayBuilder b, T obj, long ts);

    abstract public T fromByteArray(ByteArrayBuilder b, long ts);
}
