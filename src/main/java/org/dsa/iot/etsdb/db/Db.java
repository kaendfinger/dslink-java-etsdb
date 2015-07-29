package org.dsa.iot.etsdb.db;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.NodeUtils;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.utils.QueryData;
import org.etsdb.DatabaseFactory;
import org.etsdb.QueryCallback;
import org.etsdb.impl.DatabaseImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Samuel Grenier
 */
public class Db extends Database {

    private static final Logger LOGGER = LoggerFactory.getLogger(Db.class);
    private final DbProvider provider;
    private final String path;
    private final File fPath;

    private DatabaseImpl<Value> db;
    private boolean purgeable;
    private long diskSpaceRemaining;
    private ScheduledFuture<?> diskUsedMonitor;
    private ScheduledFuture<?> diskFreeMonitor;

    public Db(String name, String path, DbProvider provider) {
        super(name, provider);
        this.provider = provider;
        this.path = path;
        this.fPath = new File(path);
    }

    public DatabaseImpl<Value> getDb() {
        return db;
    }

    public File getPath() {
        return fPath;
    }

    public boolean isPurgeable() {
        return purgeable;
    }

    public long getDiskSpaceRemaining() {
        return diskSpaceRemaining;
    }

    private void setDiskSpaceRemaining(int space) {
        long totalSize = fPath.getTotalSpace();
        this.diskSpaceRemaining = totalSize * (space / 100);
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
        provider.getPurger().addDb(this);
    }

    @Override
    public void close() throws Exception {
        provider.getPurger().removeDb(this);
        try {
            db.close();
        } finally {
            diskUsedMonitor.cancel(false);
            diskFreeMonitor.cancel(false);
        }
    }

    @Override
    public void initExtensions(final Node parent) {
        {
            NodeBuilder b = parent.createChild("edit");
            b.setDisplayName("Edit");
            b.setRoConfig("ap", new Value(true));
            b.setRoConfig("dsr", new Value(10));
            {
                final Parameter purgeParam;
                {
                    purgeParam = new Parameter("Auto Purge", ValueType.BOOL);
                    Value def = NodeUtils.getRoConfig(b, "ap");
                    purgeParam.setDefaultValue(def);
                    {
                        String desc = "Whether the database is allowed to ";
                        desc += "be purged automatically.";
                        purgeParam.setDescription(desc);
                    }
                }

                final Parameter spaceParam;
                {
                    spaceParam = new Parameter("Disk Space Remaining", ValueType.NUMBER);
                    Value def = NodeUtils.getRoConfig(b, "dsr");
                    spaceParam.setDefaultValue(def);
                    {
                        String desc = "Controls how much disk space should be ";
                        desc += "remaining before a purge gets ran based on ";
                        desc += "a percentage of the total disk space on the ";
                        desc += "partition that the database is stored on";
                        spaceParam.setDescription(desc);
                    }
                }

                EditSettingsHandler handler = new EditSettingsHandler();

                Action a = new Action(getProvider().dbPermission(), handler);
                a.addParameter(purgeParam);
                a.addParameter(spaceParam);

                handler.setAction(a);
                handler.setPurgeParam(purgeParam);
                handler.setDiskParam(spaceParam);

                b.setAction(a);
            }
            Node node = b.build();
            purgeable = node.getRoConfig("ap").getBool();
            setDiskSpaceRemaining(node.getRoConfig("dsr").getNumber().intValue());
        }

        {
            NodeBuilder b = parent.createChild("dap");
            b.setDisplayName("Delete and Purge");
            b.setAction(new Action(getProvider().dbPermission(),
                    new Handler<ActionResult>() {
                        @Override
                        public void handle(ActionResult event) {
                            getProvider().deleteDb(parent);
                            deleteDirectory(fPath);
                        }
                    }));
            b.build();
        }

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

        {
            NodeBuilder b = parent.createChild("dbs");
            b.setDisplayName("Database Size");
            b.setValueType(ValueType.NUMBER);
            b.setConfig("unit", new Value("MiB"));
            final Node node;
            {
                Node tmp = b.getParent().getChild("dbs");
                if (tmp == null) {
                    node = b.getChild();
                } else {
                    node = tmp;
                }
            }

            ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
            diskUsedMonitor = stpe.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    double size = db.getDatabaseSize();
                    size /= 1048576;
                    node.setValue(new Value(size));

                }
            }, 0, 5, TimeUnit.SECONDS);
            b.build();
        }
        {
            NodeBuilder b = parent.createChild("sa");
            b.setDisplayName("Space Available");
            b.setValueType(ValueType.NUMBER);
            b.setConfig("unit", new Value("MiB"));
            final Node node;
            {
                Node tmp = b.getParent().getChild("sa");
                if (tmp == null) {
                    node = b.getChild();
                } else {
                    node = tmp;
                }
            }

            ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
            diskFreeMonitor = stpe.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    double size = db.availableSpace();
                    size /= 1048576;
                    node.setValue(new Value(size));

                }
            }, 0, 5, TimeUnit.SECONDS);
            b.build();
        }
    }

    private static void deleteDirectory(File path) {
        File[] files = path.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isFile()) {
                    if (!f.delete()) {
                        LOGGER.error("Failed to delete: {}", f.getAbsolutePath());
                    }
                } else {
                    deleteDirectory(f);
                }
            }
        }
        if (!path.delete()) {
            LOGGER.error("Failed to delete: {}", path.getAbsolutePath());
        }
    }

    private class EditSettingsHandler implements Handler<ActionResult> {

        private Action action;
        private Parameter purgeParam;
        private Parameter diskParam;

        private void setAction(Action a) {
            this.action = a;
        }

        private void setPurgeParam(Parameter p) {
            this.purgeParam = p;
        }

        private void setDiskParam(Parameter p) {
            this.diskParam = p;
        }

        @Override
        public void handle(ActionResult event) {
            Node node = event.getNode();

            Value vP = event.getParameter("Auto Purge", ValueType.BOOL);
            node.setRoConfig("ap", vP);

            Value vD = event.getParameter("Disk Space Remaining", ValueType.NUMBER);
            if (vD.getNumber().intValue() < 0) {
                vD.set(0);
            } else if (vD.getNumber().intValue() > 100) {
                vD.set(100);
            }
            node.setRoConfig("dsr", vD);

            purgeable = vP.getBool();
            purgeParam.setDefaultValue(vP);

            setDiskSpaceRemaining(vD.getNumber().intValue());
            diskParam.setDefaultValue(vD);

            List<Parameter> params = new ArrayList<>();
            params.add(purgeParam);
            params.add(diskParam);
            action.setParams(params);
        }
    }
}
