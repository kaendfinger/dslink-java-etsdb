package org.etsdb;

import org.etsdb.impl.Utils;

public class TimeRange {
    private long from;
    private long to;

    public TimeRange() {
        from = Long.MAX_VALUE;
        to = -Long.MAX_VALUE;
    }

    public TimeRange(long from, long to) {
        this.from = from;
        this.to = to;
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

    public TimeRange intersection(TimeRange that) {
        return intersection(that.from, that.to);
    }

    public TimeRange intersection(long from, long to) {
        TimeRange result = new TimeRange();

        if (this.to < from || to < this.from)
            return result;

        result.setFrom(from < this.from ? this.from : from);
        result.setTo(to > this.to ? this.to : to);
        return result;
    }

    public String toSimpleString() {
        return "TimeRange [from=" + from + ", to=" + to + "]";
    }

    @Override
    public String toString() {
        if (isUndefined())
            return "TimeRange [undefined]";
        StringBuilder sb = new StringBuilder();
        sb.append("TimeRange [from=");
        sb.append(Utils.prettyTimestamp(from));
        sb.append(", to=");
        sb.append(Utils.prettyTimestamp(to));
        sb.append("]");
        return sb.toString();
    }
}
