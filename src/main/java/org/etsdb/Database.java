package org.etsdb;

import org.vertx.java.core.Handler;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * TODO seriesIds must be valid file names. Consider adding code to convert invalid characters to something valid.
 * TODO consider a flushing scheme that writes to files according to a set schedule, e.g. one file per second.
 * TODO add a query that returns the given time range, plus the pvts immediately before and after the range. Currently
 * this is three queries.
 * <p>
 * watch grep ^Cached /proc/meminfo # Page Cache size
 * watch grep -A 1 dirty /proc/vmstat # Dirty Pages and writeback to disk activity
 * watch cat /proc/sys/vm/nr_pdflush_threads # shows # of active pdflush threads
 *
 * @param <T> The type of value that is stored in the database. E.g. Double, Integer, DataValue,
 *            MyArbitraryValueThatMightBeNumericOrTextOrArrayOrObject
 * @author mlohbihler
 */
public interface Database<T> {
    File getBaseDir();

    void renameSeries(String fromId, String toId);

    void write(String seriesId, long ts, T value);

    void query(String seriesId, long fromTs, long toTs, final QueryCallback<T> cb);

    void query(String seriesId, long fromTs, long toTs, int limit, final QueryCallback<T> cb);

    void query(String seriesId, long fromTs, long toTs, boolean reverse, final QueryCallback<T> cb);

    void query(String seriesId, long fromTs, long toTs, int limit, boolean reverse, final QueryCallback<T> cb);

    void wideQuery(String seriesId, long fromTs, long toTs, int limit, boolean reverse, final WideQueryCallback<T> cb);

    void multiQuery(List<String> seriesIds, long fromTs, long toTs, final QueryCallback<T> cb);

    long count(String seriesId, long fromTs, long toTs);

    List<String> getSeriesIds();

    long getDatabaseSize();

    long availableSpace();

    TimeRange getTimeRange(String... seriesId);

    TimeRange getTimeRange(List<String> seriesIds);

    long delete(String seriesId, long fromTs, long toTs);

    void purge(String seriesId, long toTs);

    /**
     * ??? There are potential concurrency problems with this method, since we do not actually lock Series objects.
     * This should only be run when there is certainty that no writes or queries will be run at the same time.
     *
     * @param seriesId
     */
    void deleteSeries(String seriesId);

    void backup(String filename) throws IOException;

    void close() throws IOException;

    //
    //
    // Metrics
    //
    int getWritesPerSecond();

    void setWritesPerSecondHandler(Handler<Integer> handler);

    long getWriteCount();

    void setWriteCountHandler(Handler<Long> handler);

    long getFlushCount();

    void setFlushCountHandler(Handler<Long> handler);

    long getBackdateCount();

    void setBackdateCountHandler(Handler<Long> handler);

    int getOpenFiles();

    void setOpenFilesHandler(Handler<Integer> handler);

    long getFlushForced();

    void setFlushForcedHandler(Handler<Long> handler);

    long getFlushExpired();

    void setFlushExpiredHandler(Handler<Long> handler);

    long getFlushLimit();

    void setFlushLimitHandler(Handler<Long> handler);

    long getForcedClose();

    int getLastFlushMillis();

    void setLastFlushMillisHandler(Handler<Integer> handler);

    int getQueueSize();

    void setQueueSizeHandler(Handler<Integer> handler);

    int getOpenShards();

    void setOpenShardsHandler(Handler<Integer> handler);
}
