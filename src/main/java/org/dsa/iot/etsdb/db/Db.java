package org.dsa.iot.etsdb.db;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.database.DatabaseProvider;
import org.dsa.iot.historian.utils.QueryData;
import org.etsdb.DatabaseFactory;
import org.etsdb.QueryCallback;
import org.vertx.java.core.Handler;

import java.io.File;

/**
 * @author Samuel Grenier
 */
public class Db extends Database {

    private org.etsdb.Database<Value> db;
    private final String path;

    public Db(String name, String path, DatabaseProvider provider) {
        super(name, provider);
        this.path = path;
    }

    @Override
    public void write(String path, Value value, long ts) {
        db.write(path, ts, value);
    }

    @Override
    public void query(String path,
                      long from,
                      long to,
                      final Handler<QueryData> handler) {
        db.query(path, from, to, new QueryCallback<Value>() {
            @Override
            public void sample(String seriesId, long ts, Value value) {
                handler.handle(new QueryData(value, ts));
            }
        });
    }

    @Override
    public QueryData queryFirst(String path) {
        final QueryData data = new QueryData();
        db.query(path, Long.MIN_VALUE,
                Long.MAX_VALUE, 1, new QueryCallback<Value>() {
            @Override
            public void sample(String seriesId, long ts, Value value) {
                data.setTimestamp(ts);
                data.setValue(value);
            }
        });
        return data;
    }

    @Override
    public QueryData queryLast(String path) {
        final QueryData data = new QueryData();
        db.query(path, Long.MIN_VALUE,
                Long.MAX_VALUE, 1, true, new QueryCallback<Value>() {
            @Override
            public void sample(String seriesId, long ts, Value value) {
                data.setTimestamp(ts);
                data.setValue(value);
            }
        });
        return data;
    }

    @Override
    protected void performConnect() throws Exception {
        File d = new File(path);
        ValueSerializer vs = new ValueSerializer();
        db = DatabaseFactory.createDatabase(d, vs);
    }

    @Override
    protected void close() throws Exception {
        db.close();
    }

    @Override
    public void initExtensions(Node parent) {
        {
            NodeBuilder b = parent.createChild("wps");
            b.setDisplayName("Writes Per Second");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getWritesPerSecond()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setWritesPerSecondHandler(new Handler<Integer>() {
                @Override
                public void handle(Integer event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("rw");
            b.setDisplayName("Rows Written");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getBackdateCount()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setWriteCountHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("rf");
            b.setDisplayName("Rows Flushed");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getFlushCount()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setFlushCountHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("rc");
            b.setDisplayName("Rows Cached");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getQueueSize()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setQueueSizeHandler(new Handler<Integer>() {
                @Override
                public void handle(Integer event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("fcf");
            b.setDisplayName("Forced Rows Flushed");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getFlushForced()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setFlushForcedHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("cef");
            b.setDisplayName("Cache Expiry Flushes");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getFlushExpired()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setFlushExpiredHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("clf");
            b.setDisplayName("Cache Limit Flushes");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getFlushLimit()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setFlushLimitHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("lfd");
            b.setDisplayName("Last Flush Duration");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getLastFlushMillis()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setLastFlushMillisHandler(new Handler<Integer>() {
                @Override
                public void handle(Integer event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("brw");
            b.setDisplayName("Backdated Rows written");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getBackdateCount()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setBackdateCountHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("os");
            b.setDisplayName("Open Shards");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getOpenShards()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setOpenShardsHandler(new Handler<Integer>() {
                @Override
                public void handle(Integer event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("of");
            b.setDisplayName("Open Files");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getOpenFiles()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setOpenFilesHandler(new Handler<Integer>() {
                @Override
                public void handle(Integer event) {
                    node.setValue(new Value(event));
                }
            });
        }
    }
}
