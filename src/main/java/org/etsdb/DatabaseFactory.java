package org.etsdb;

import org.etsdb.impl.DatabaseImpl;

import java.io.File;

public class DatabaseFactory {
    public static <T> DatabaseImpl<T> createDatabase(File baseDir, Serializer<T> serializer) {
        return createDatabase(baseDir, serializer, null);
    }

    public static <T> DatabaseImpl<T> createDatabase(File baseDir, Serializer<T> serializer, DbConfig config) {
        return new DatabaseImpl<>(baseDir, serializer, config);
    }
}
