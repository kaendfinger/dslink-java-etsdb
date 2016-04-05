package org.etsdb.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A queue for writing backdated samples.
 * <p>
 *
 * @author Matthew
 */
class Backdates {
    static final Logger LOGGER = LoggerFactory.getLogger(Backdates.class.getName());

    final DatabaseImpl<?> db;
    final int startDelay;
    final List<Backdate> backdates = new LinkedList<>();

    BackdatePoster poster;

    Backdates(DatabaseImpl<?> db, int startDelay) {
        this.db = db;
        this.startDelay = startDelay;
    }

    void add(Backdate backdate) {
        synchronized (backdates) {
            backdates.add(backdate);
            if (poster == null) {
                poster = new BackdatePoster();
            }
        }
    }

    void close() {
        BackdatePoster poster;
        synchronized (backdates) {
            poster = this.poster;
        }

        if (poster != null) {
            // Break the poster out of its start wait in case that's what it is doing.
            poster.notify();
            poster.join();
        }
    }

    private class BackdatePoster implements Runnable {
        private final Thread thread;

        BackdatePoster() {
            thread = new Thread(this, "ETSDB Backdate Poster");
            thread.setPriority(Thread.MAX_PRIORITY - 1);
            thread.start();
        }

        @Override public void run() {
            try {
                runImpl();
            } catch (Exception e) {
                LOGGER.warn("Backdate poster failed with exception", e);
            }
        }

        private void runImpl() throws Exception {
            if (startDelay > 0) {
                synchronized (this) {
                    wait(startDelay);
                }
            }

            List<Backdate> shardInserts = new ArrayList<>();
            while (true) {
                Backdate first;

                synchronized (backdates) {
                    if (backdates.isEmpty()) {
                        poster = null;
                        break;
                    }

                    first = backdates.remove(0);
                    shardInserts.add(first);

                    // Collect all of the backdates for the same shard.
                    Iterator<Backdate> iter = backdates.iterator();
                    while (iter.hasNext()) {
                        Backdate bd = iter.next();
                        if (bd.getSeriesId().equals(first.getSeriesId()) && bd.getShardId() == first.getShardId()) {
                            iter.remove();
                            shardInserts.add(bd);
                        }
                    }
                }

                // Sort the shard inserts.
                Collections.sort(shardInserts);

                // Insert the backdates into the shard.
                db.insert(first.getSeriesId(), first.getShardId(), shardInserts);

                shardInserts.clear();
            }

            synchronized (backdates) {
                poster = null;
            }
        }

        void join() {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // no op
            }
        }
    }
}
