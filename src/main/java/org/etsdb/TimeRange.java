package org.etsdb;

import org.etsdb.impl.Utils;

public class TimeRange {
    private long from;
    private long to;

    public TimeRange() {
        from = Long.MAX_VALUE;
        to = -Long.MAX_VALUE;
    }

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }

    public boolean isUndefined() {
        return from == Long.MAX_VALUE;
    }

    public void add(long ts) {
        if (from > ts)
            from = ts;
        if (to < ts)
            to = ts;
    }

    public TimeRange union(TimeRange that) {
        TimeRange result = new TimeRange();
        result.setFrom(that.from < from ? that.from : from);
        result.setTo(that.to > to ? that.to : to);
        return result;
    }

    @Override
    public String toString() {
        if (isUndefined())
            return "TimeRange [undefined]";
        return "TimeRange [from=" +
                Utils.prettyTimestamp(from) +
                ", to=" +
                Utils.prettyTimestamp(to) +
                "]";
    }
}
