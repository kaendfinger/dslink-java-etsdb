package org.etsdb.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * Modified from the H2 FileLock implementation.
 *
 * @author Matthew
 */
class FileLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileLock.class.getName());

    private static final int SLEEP_GAP = 25;
    private static final int TIME_GRANULARITY = 2000;
    private final int sleep;
    private File file;
    /**
     * The last time the lock file was written.
     */
    private long lastWrite;

    private boolean locked;
    private String uniqueId;

    /**
     * Create a new file locking object.
     *
     * @param db    Database implementation to use
     * @param sleep the number of milliseconds to sleep
     */
    FileLock(DatabaseImpl<?> db, int sleep) {
        this.file = new File(db.getBaseDir(), ".lock.db");
        this.sleep = sleep;
    }

    private static void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    /**
     * Lock the file if possible. A file may only be locked once.
     *
     * @throws RuntimeException if locking was not successful
     */
    synchronized void lock() {
        if (locked) {
            throw new RuntimeException("already locked");
        }
        try {
            lockFile();
        } catch (IOException e) {
            throw new RuntimeException("Error locking file");
        }
        locked = true;
    }

    /**
     * Unlock the file. The watchdog thread is stopped. This method does nothing
     * if the file is already unlocked.
     */
    synchronized void unlock() {
        if (!locked) {
            return;
        }

        locked = false;
        try {
            if (file != null) {
                if (load().equals(uniqueId)) {
                    if (!file.delete()) {
                        LOGGER.error("Failed to delete file: {}", file.getPath());
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("FileLock.unlock", e);
        } finally {
            file = null;
        }
    }

    /**
     * Save the lock file.
     */
    private synchronized void save() {
        try {
            FileOutputStream out = null;
            try {
                out = new FileOutputStream(file);
                out.write(uniqueId.getBytes("UTF-16"));
            } finally {
                Utils.closeQuietly(out);
            }

            lastWrite = file.lastModified();
        } catch (IOException e) {
            throw new RuntimeException("Could not save properties " + file, e);
        }
    }

    /**
     * Load the properties file.
     *
     * @return the properties
     */
    private String load() {
        FileInputStream in = null;
        try {
            synchronized (this) {
                in = new FileInputStream(file);
            }
            byte[] b = new byte[128];
            int position = 0;
            int count;
            while ((count = in.read(b, position, b.length - position)) != -1) {
                position += count;
            }
            return new String(b, 0, position, "UTF-16");
        } catch (IOException e) {
            throw new RuntimeException("Could not load lock file", e);
        } finally {
            Utils.closeQuietly(in);
        }
    }

    private void waitUntilOld() {
        for (int i = 0; i < 2 * TIME_GRANULARITY / SLEEP_GAP; i++) {
            long last;
            synchronized (this) {
                last = file.lastModified();
            }
            long dist = System.currentTimeMillis() - last;
            if (dist < -TIME_GRANULARITY) {
                // lock file modified in the future -
                // wait for a bit longer than usual
                sleep(2 * sleep);
                return;
            } else if (dist > TIME_GRANULARITY) {
                return;
            }
            sleep(SLEEP_GAP);
        }

        throw new RuntimeException("Lock file recently modified");
    }

    private synchronized void setUniqueId() {
        uniqueId = UUID.randomUUID().toString();
    }

    private synchronized void lockFile() throws IOException {
        setUniqueId();
        if (!file.createNewFile()) {
            waitUntilOld();
            save();
            sleep(2 * sleep);
            if (!load().equals(uniqueId)) {
                throw new RuntimeException("Locked by another process");
            }

            if (!file.delete()) {
                LOGGER.error("Failed to delete file: {}", file.getPath());
            }
            if (!file.createNewFile()) {
                throw new RuntimeException("Another process was faster");
            }
        }
        save();
        sleep(SLEEP_GAP);
        if (!load().equals(uniqueId)) {
            file = null;
            throw new RuntimeException("Concurrent update");
        }
    }

    synchronized void update() {
        if (file != null) {
            try {
                if (!file.exists() || file.lastModified() != lastWrite) {
                    save();
                }
            } catch (OutOfMemoryError ignored) {
            } catch (Exception e) {
                LOGGER.warn("FileLock.run", e);
            }
        }
    }
}
