package org.dsa.iot.etsdb;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.etsdb.Database;
import org.vertx.java.core.Handler;

/**
 * @author Samuel Grenier
 */
public class DatabaseStats {

    public static void init(Database<?> db, Node parent) {
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
