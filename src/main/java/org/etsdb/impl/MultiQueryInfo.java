package org.etsdb.impl;

import org.etsdb.ByteArrayBuilder;

import java.io.IOException;

public class MultiQueryInfo<T> {
    private final Series<T> series;
    private final long fromTs;
    private final long toTs;
    private final long toShard;

    private final ScanInfo scanInfo = new ScanInfo();
    private ChecksumInputStream in;

    // Current values.
    private long shardId;
    private long fromOffset;
    private long toOffset;
    private DataShard shard;

    public MultiQueryInfo(Series<T> series, long fromTs, long toTs, long fromShard, long toShard) {
        this.series = series;
        this.fromTs = fromTs;
        this.toTs = toTs;
        this.toShard = toShard;
        this.shardId = fromShard;
    }

    public boolean next() throws IOException {
        while (shardId <= toShard) {
            if (shard == null) {
                shard = series.multiQueryOpenShard(shardId);
                scanInfo.reset(shard);
                in = shard.multiQueryOpen();

                fromOffset = Utils.getOffsetInShard(shardId, fromTs);
                toOffset = Utils.getOffsetInShard(shardId, toTs);
            }

            shard.multiQueryNext(in, scanInfo);

            if (!scanInfo.isEndOfShard()) {
                if (scanInfo.getOffset() < fromOffset)
                    continue; // Ignore. Before time range
                else if (scanInfo.getOffset() >= toOffset) {
                    shard.multiQueryClose(in);
                    series.multiQueryCloseShard(shard);
                    break; // After time range. Done.
                } else
                    return true;
            }

            shard.multiQueryClose(in);
            series.multiQueryCloseShard(shard);
            shard = null;

            shardId++;
            if (shardId > toShard)
                break;
        }

        return false;
    }

    public String getSeriesId() {
        return series.getId();
    }

    public long getTs() {
        return Utils.getTimestamp(shardId, scanInfo.getOffset());
    }

    public ByteArrayBuilder getData() {
        return scanInfo.getData();
    }
}
