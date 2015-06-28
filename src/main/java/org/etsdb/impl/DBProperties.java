package org.etsdb.impl;

import org.etsdb.EtsdbException;
import org.etsdb.util.AbstractProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class DBProperties extends AbstractProperties {
    private static final String FILENAME = "db.properties";

    private final DatabaseImpl<?> db;
    private final Properties props;

    public DBProperties(DatabaseImpl<?> db) {
        this.db = db;
        props = new Properties();

        File file = getFile();
        if (file.exists()) {
            try {
                FileInputStream in = new FileInputStream(file);
                try {
                    props.load(in);
                } finally {
                    Utils.closeQuietly(in);
                }
            } catch (IOException e) {
                throw new EtsdbException(e);
            }
        }
    }

    @Override
    protected String getStringImpl(String key) {
        return props.getProperty(key);
    }

    public void setInt(String key, int value) {
        setString(key, Integer.toString(value));
    }

    public void setBoolean(String key, boolean value) {
        setString(key, Boolean.toString(value));
    }

    public void setString(String key, String value) {
        props.setProperty(key, value);

        FileOutputStream out = null;
        try {
            out = new FileOutputStream(getFile());
            props.store(out, "");
        } catch (IOException e) {
            throw new EtsdbException(e);
        } finally {
            Utils.closeQuietly(out);
        }
    }

    File getFile() {
        return new File(db.getBaseDir(), FILENAME);
    }
}
