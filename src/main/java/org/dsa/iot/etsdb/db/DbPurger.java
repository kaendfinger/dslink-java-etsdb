package org.dsa.iot.etsdb.db;

import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.Objects;
import org.etsdb.TimeRange;
import org.etsdb.impl.DatabaseImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Manages whether a database should be purged or not.
 *
 * @author Samuel Grenier
 */
public class DbPurger {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbPurger.class);
    private List<Db> databases = new ArrayList<>();

    public synchronized void addDb(Db db) {
        if (!databases.contains(db)) {
            databases.add(db);
        }
    }

    public synchronized void removeDb(Db db) {
        databases.remove(db);
    }

    void setupPurger() {
        Objects.getDaemonThreadPool().scheduleWithFixedDelay(new Runnable() {
            @Override
            public synchronized void run() {
                for (Db db : databases) {
                    if (!db.isPurgeable()) {
                        continue;
                    }
                    File path = db.getPath();
                    long curr = path.getUsableSpace();
                    long request = db.getDiskSpaceRemaining();
                    long delCount = 0;
                    while (curr - request <= 0) {
                        DatabaseImpl<Value> realDb = db.getDb();

                        List<String> series = realDb.getSeriesIds();
                        TimeRange range = realDb.getTimeRange(series);
                        if (range == null || range.isUndefined()) {
                            break;
                        }

                        long from = range.getFrom();
                        for (String s : series) {
                            delCount += realDb.delete(s, from, from + 1);
                        }

                        if (delCount <= 0) {
                            break;
                        }
                        curr = path.getUsableSpace();
                    }
                    if (delCount > 0) {
                        String p = path.getPath();
                        LOGGER.info("Deleted {} records from {}", delCount, p);
                    }
                }
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
}
