package org.etsdb.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class NonconcurrentRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(NonconcurrentRunnable.class.getName());

    protected final DatabaseImpl<?> db;
    private Thread thread;

    public NonconcurrentRunnable(DatabaseImpl<?> db) {
        this.db = db;
    }

    @Override
    public final void run() {
        if (thread != null) {
            logger.warn(getClass() + " did not run at " + System.currentTimeMillis()
                    + " because another run of it is still in progress.");
            return;
        }

        thread = Thread.currentThread();
        try {
            runNonConcurrent();
        } finally {
            thread = null;
        }
    }

    abstract protected void runNonConcurrent();
}
