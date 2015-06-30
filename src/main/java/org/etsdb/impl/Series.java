package org.etsdb.impl;

import org.etsdb.ByteArrayBuilder;
import org.etsdb.Serializer;
import org.etsdb.TimeRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Series<T> {
    private static final Logger logger = LoggerFactory.getLogger(Series.class.getName());

    private final DatabaseImpl<T> db;
    private final File seriesDir;
    private final String id;
    private final Serializer<T> serializer;

    private final ByteArrayBuilder buffer = new ByteArrayBuilder();
    private final Map<Long, DataShard> shardLookup = new HashMap<>();
    private long minShard = Long.MAX_VALUE;
    private long maxShard = 0;

    Series(DatabaseImpl<T> db, File baseDir, String id, Serializer<T> serializer) {
        this.db = db;
        seriesDir = Utils.getSeriesDir(baseDir, id);
        if (!(seriesDir.exists() || seriesDir.mkdirs())) {
            logger.error("Failed to create seriesDir: {}", seriesDir.getPath());
        }
        this.id = id;
        this.serializer = serializer;

        String[] shards = seriesDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".data") || name.endsWith(".meta");
            }
        });
        if (shards != null) {
            for (String shard : shards) {
                try {
                    // Remove the .data extension
                    shard = shard.substring(0, shard.length() - 5);
                    updateMinMax(Long.parseLong(shard));
                } catch (NumberFormatException e) {
                    // no op
                }
            }
        }
    }

    String getId() {
        return id;
    }

    void write(long ts, T value) throws IOException {
        synchronized (buffer) {
            buffer.clear();
            serializer.toByteArray(buffer, value, ts);
            write(ts, buffer.getBuffer(), buffer.getReadOffset(), buffer.getAvailable());
        }
    }

    void write(long ts, byte[] data) throws IOException {
        write(ts, data, 0, data.length);
    }

    private void write(long ts, byte[] data, int off, int len) throws IOException {
        DataShard shard = getShard(ts, true);
        try {
            shard.write(ts, data, off, len);
            checkOpenFiles(shard);
        } finally {
            shard.unlockWrite();
        }
    }

    void insert(long shardId, List<Backdate> backdates) throws IOException {
        DataShard shard = getShardById(shardId, true);
        try {
            shard.insertSamples(backdates);
        } finally {
            shard.unlockWrite();
        }
    }

    void query(long fromTs, long toTs, int limit, boolean reverse, RawQueryCallback cb) throws IOException {
        // Determine the shard range to query
        long fromShard = Utils.getShardId(fromTs);
        long toShard = Utils.getShardId(toTs);
        synchronized (shardLookup) {
            if (fromShard < minShard)
                fromShard = minShard;
            if (toShard > maxShard)
                toShard = maxShard;
        }

        // Iterate through the shards.
        for (long sid = fromShard; sid <= toShard; sid++) {
            long shardId = sid;
            if (reverse)
                shardId = toShard - sid + fromShard;

            // Get a handle on the current shard.
            DataShard shard = getShardById(shardId, false);
            try {
                long fromOffset = Utils.getOffsetInShard(shardId, fromTs);
                long toOffset = Utils.getOffsetInShard(shardId, toTs);

                int count;
                if (reverse)
                    count = shard.queryReverse(fromOffset, toOffset, limit, cb);
                else
                    count = shard.query(fromOffset, toOffset, limit, cb);

                if (limit != Integer.MAX_VALUE) {
                    limit -= count;
                    if (limit <= 0)
                        // We found all the rows we need. Break from the shard loop.
                        break;
                }
            } finally {
                shard.unlockRead();
            }
        }
    }

    void wideQuery(long fromTs, long toTs, int limit, boolean reverse, RawWideQueryCallback cb) throws IOException {
        // TODO
        //        // Determine the shard range to query
        //        long fromShard = Utils.getShardId(fromTs);
        //        long toShard = Utils.getShardId(toTs);
        //        synchronized (shardLookup) {
        //            if (fromShard < minShard)
        //                fromShard = minShard;
        //            if (toShard > maxShard)
        //                toShard = maxShard;
        //        }
        //
        //        // Iterate through the shards.
        //        for (long sid = fromShard; sid <= toShard; sid++) {
        //            long shardId = sid;
        //            if (reverse)
        //                shardId = toShard - sid + fromShard;
        //
        //            // Get a handle on the current shard.
        //            DataShard shard = getShardById(shardId, false);
        //            try {
        //                long fromOffset = Utils.getOffsetInShard(shardId, fromTs);
        //                long toOffset = Utils.getOffsetInShard(shardId, toTs);
        //
        //                int count;
        //                if (reverse)
        //                    count = shard.queryReverse(fromOffset, toOffset, limit, cb);
        //                else
        //                    count = shard.query(fromOffset, toOffset, limit, cb);
        //
        //                if (limit != Integer.MAX_VALUE) {
        //                    limit -= count;
        //                    if (limit <= 0)
        //                        // We found all the rows we need. Break from the shard loop.
        //                        break;
        //                }
        //            }
        //            finally {
        //                shard.unlockRead();
        //            }
        //        }
    }

    TimeRange getTimeRange() throws IOException {
        long minShard, maxShard;

        synchronized (shardLookup) {
            minShard = this.minShard;
            maxShard = this.maxShard;
        }

        if (maxShard == 0)
            return null;

        TimeRange range = new TimeRange();

        DataShard min = getShardById(minShard, false);
        try {
            range.setFrom(min.getMinTs());
            if (minShard == maxShard)
                range.setTo(min.getMaxTs());
            else {
                DataShard max = getShardById(maxShard, false);
                try {
                    range.setTo(max.getMaxTs());
                } finally {
                    max.unlockRead();
                }
            }
        } finally {
            min.unlockRead();
        }

        return range;
    }

    void delete(long fromTs, long toTs) {
        // TODO
    }

    void purge(long toTs) {
        long toShard = Utils.getShardId(toTs);

        if (toShard <= minShard)
            return;
        if (toShard > maxShard)
            toShard = maxShard + 1;

        synchronized (shardLookup) {
            for (long shardId = minShard; shardId < toShard; shardId++) {
                DataShard shard = shardLookup.get(shardId);
                if (shard != null) {
                    try {
                        shard.lockWrite();
                        shard.close();
                        shardLookup.remove(shardId);
                        db.openShards.decrementAndGet();

                        try {
                            Utils.deleteWithRetry(new File(seriesDir, shardId + ".meta"));
                        } catch (IOException e) {
                            logger.warn("Error while deleting shard meta " + shardId + " in series " + id, e);
                        }

                        try {
                            Utils.deleteWithRetry(new File(seriesDir, shardId + ".data"));
                        } catch (IOException e) {
                            logger.warn("Error while deleting shard data " + shardId + " in series " + id, e);
                        }
                    } finally {
                        shard.unlockWrite();
                    }
                }
            }

            if (toShard > maxShard) {
                minShard = Long.MAX_VALUE;
                maxShard = 0;
            } else
                minShard = toShard;
        }
    }

    int flush(long runtime, boolean force) throws IOException {
        int closures = 0;

        for (DataShard shard : getShards()) {
            try {
                shard.lockWrite();
                shard.flush(runtime, force);
                checkOpenFiles(shard);
                closures = shard.resetMetaClosures();
                if (shard.isClosed()) {
                    synchronized (shardLookup) {
                        shardLookup.remove(shard.getShardId());
                        db.openShards.decrementAndGet();
                    }
                }
            } finally {
                shard.unlockWrite();
            }
        }

        synchronized (buffer) {
            synchronized (shardLookup) {
                if (shardLookup.isEmpty()) {
                    buffer.resetCapacity();
                }
            }
        }

        return closures;
    }

    private void checkOpenFiles(DataShard shard) {
        if (db.tooManyFiles()) {
            shard.closeFiles();
            db.forcedClose.incrementAndGet();
        }
    }

    void close() {
        for (DataShard shard : getShards()) {
            try {
                shard.lockWrite();
                shard.close();
                synchronized (shardLookup) {
                    shardLookup.remove(shard.getShardId());
                    db.openShards.decrementAndGet();
                }
            } finally {
                shard.unlockWrite();
            }
        }
    }

    private List<DataShard> getShards() {
        List<DataShard> shards;
        synchronized (shardLookup) {
            shards = new ArrayList<>(shardLookup.values());
        }
        return shards;
    }

    //
    //
    // Multi-query
    //
    MultiQueryInfo<T> multiQueryOpen(long fromTs, long toTs) {
        // Determine the shard range to query
        long fromShard = Utils.getShardId(fromTs);
        long toShard = Utils.getShardId(toTs);
        synchronized (shardLookup) {
            if (fromShard < minShard)
                fromShard = minShard;
            if (toShard > maxShard)
                toShard = maxShard;
        }

        return new MultiQueryInfo<>(this, fromTs, toTs, fromShard, toShard);
    }

    DataShard multiQueryOpenShard(long shardId) throws IOException {
        return getShardById(shardId, false);
    }

    void multiQueryCloseShard(DataShard shard) {
        shard.unlockRead();
    }

    //
    //
    // Private
    //

    private DataShard getShard(long ts, boolean writeLock) throws IOException {
        return getShardById(Utils.getShardId(ts), writeLock);
    }

    private DataShard getShardById(long shardId, boolean writeLock) throws IOException {
        // Enter the retry loop.
        int attempts = 10;
        while (attempts > 0) {
            // Get the shard.
            DataShard shard = _getShardById(shardId);

            // Lock it appropriately
            if (writeLock)
                shard.lockWrite();
            else
                shard.lockRead();

            // Check if it is already closed.
            if (!shard.isClosed())
                // If not, we're good, so just return it.
                return shard;

            // Unlock the shard.
            if (writeLock)
                shard.unlockWrite();
            else
                shard.unlockRead();

            // Try again to get a shard that is not already closed.
            attempts--;
        }

        // Too many attempts. Just give up.
        throw new IOException("Failed to get unclosed shard in series " + id + ", shard " + shardId);
    }

    private DataShard _getShardById(long shardId) throws IOException {
        DataShard shard = shardLookup.get(shardId);
        if (shard == null) {
            synchronized (shardLookup) {
                shard = shardLookup.get(shardId);
                if (shard == null) {
                    shard = new DataShard(db, seriesDir, id, shardId);
                    shardLookup.put(shardId, shard);
                    db.openShards.incrementAndGet();
                    updateMinMax(shardId);
                }
            }
        }
        return shard;
    }

    private void updateMinMax(long shardId) {
        if (minShard > shardId)
            minShard = shardId;
        if (maxShard < shardId)
            maxShard = shardId;
    }
}
