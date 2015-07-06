package org.dsa.iot.etsdb.etsdb;

import org.dsa.iot.dslink.link.Requester;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.SubscriptionValue;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValuePair;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.etsdb.utils.LinkPair;
import org.dsa.iot.etsdb.utils.PathValuePair;
import org.dsa.iot.etsdb.utils.TimeParser;
import org.etsdb.Database;
import org.etsdb.QueryCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

import java.lang.ref.WeakReference;

/**
 * @author Samuel Grenier
 */
public class Watch implements Handler<SubscriptionValue> {

    private static final Logger LOGGER;
    private final WeakReference<WatchGroup> group;
    private final Node dataNode;
    private final String path;

    private final Node watchNode;
    private final Node realTimeNode;
    private final Node lastWrittenNode;

    private final Node startNode;
    private final Node endNode;

    // Data tracking
    private long lastIntervalUpdate;

    public Watch(WatchGroup group,
                 Node watchNode,
                 String path) {
        this.group = new WeakReference<>(group);
        this.watchNode = watchNode;
        this.dataNode = DataNode.initNodeFromPath(group, path);
        this.path = path;

        realTimeNode = watchNode.createChild("realTimeValue").build();
        lastWrittenNode = watchNode.createChild("lastWrittenValue").build();
        startNode = watchNode.createChild("startDate").build();
        endNode = watchNode.createChild("endDate").build();

        setPathValue();
        initUnsubscribe();
        initNameActions();
        initRealTimeValue();
        initDbValue();
        initStartValue();
        initEndValue();
    }

    public Node getDataNode() {
        return dataNode;
    }

    public void init() {
        watchNode.getListener().setValueHandler(new Handler<ValuePair>() {
            @Override
            public void handle(ValuePair event) {
                boolean prev = event.getPrevious().getBool();
                boolean curr = event.getCurrent().getBool();
                if (prev != curr) {
                    if (curr) {
                        subscribe();
                    } else {
                        unsubscribe();
                    }
                }
            }
        });

        Value val = watchNode.getValue();
        if (val.getBool()) {
            subscribe();
        }
    }

    @Override
    public void handle(SubscriptionValue event) {
        String path = event.getPath();
        Value value = event.getValue();
        if (value == null) {
            return;
        }
        String sValue = value.toString();
        dataNode.setValue(value);
        realTimeNode.setValue(value);

        LOGGER.debug("Received update for {} of {}", path, sValue);
        long time = TimeParser.parse(event.getTimestamp());
        getGroup().write(new PathValuePair(this, path, value, time));
    }

    public void subscribe() {
        LinkPair pair = getGroup().getPair();
        Requester requester = pair.getRequester().getRequester();
        if (requester.isSubscribed(path)) {
            requester.unsubscribe(path, null);
        }
        requester.subscribe(path, this);
    }

    public void unsubscribe() {
        LinkPair pair = getGroup().getPair();
        Requester req = pair.getRequester().getRequester();
        req.unsubscribe(path, null);
    }

    public void setPathValue() {
        if (watchNode.getDisplayName() != null) {
            NodeBuilder b = watchNode.createChild("path");
            b.setDisplayName("Path");
            b.setValueType(ValueType.STRING);
            b.setValue(new Value(path));
            b.getChild().setSerializable(false);
            b.build();
        } else {
            watchNode.removeChild("path");
        }
    }

    protected void setLastWrittenValue(Value value) {
        lastWrittenNode.setValue(value);
    }

    protected void setEndDate(Value value) {
        endNode.setValue(value);
    }

    protected void setLastIntervalUpdate(long time) {
        this.lastIntervalUpdate = time;
    }

    protected long getLastIntervalUpdate() {
        return lastIntervalUpdate;
    }

    private WatchGroup getGroup() {
        return group.get();
    }

    private void initRealTimeValue() {
        realTimeNode.setValueType(ValueType.DYNAMIC);
        realTimeNode.setDisplayName("Real Time Value");
    }

    private void initDbValue() {
        lastWrittenNode.setValueType(ValueType.DYNAMIC);
        lastWrittenNode.setDisplayName("Last Written Value");
    }

    protected void initUnsubscribe() {
        Node node = watchNode;
        NodeBuilder b = node.createChild("unsubscribe");
        b.setDisplayName("Unsubscribe");
        {
            Node data = getDataNode();
            WatchGroup group = getGroup();
            Action act = DataNode.getUnsubscribeAction(group, node, data, path);
            b.setAction(act);
        }
        b.getChild().setSerializable(false);
        b.build();
    }

    protected void initStartValue() {
        if (startNode.getValue() != null) {
            return;
        }
        startNode.setValueType(ValueType.TIME);
        startNode.setDisplayName("Start Date");

        // Grab the first value
        WatchGroup group = getGroup();
        Database<Value> db = group.getDb();
        db.query(path, Long.MIN_VALUE, Long.MAX_VALUE, 1, new QueryCallback<Value>() {
            @Override
            public void sample(String seriesId, long ts, Value value) {
                startNode.setValue(new Value(TimeParser.parse(ts)));
            }
        });
    }

    private void initEndValue() {
        endNode.setValueType(ValueType.TIME);
        endNode.setDisplayName("End Date");

        // Grab the first value
        WatchGroup group = getGroup();
        Database<Value> db = group.getDb();
        db.query(path, Long.MIN_VALUE, Long.MAX_VALUE, 1, true, new QueryCallback<Value>() {
            @Override
            public void sample(String seriesId, long ts, Value value) {
                endNode.setValue(new Value(TimeParser.parse(ts)));

                Node child = DataNode.initNodeFromPath(getGroup(), path);
                child.setValue(value);
            }
        });
    }

    private void initNameActions() {
        NodeBuilder b = watchNode.createChild("setDisplayName");
        b.getChild().setSerializable(false);
        b.setDisplayName("Set Display Name");

        Action act = new Action(Permission.READ, new DisplayNameSetter());
        Parameter param = new Parameter("name", ValueType.STRING);
        param.setDescription("Sets the display name of the watch path\nLeave name blank to remove the display name");
        act.addParameter(param);
        b.setAction(act);

        b.build();
    }

    private class DisplayNameSetter implements Handler<ActionResult> {

        @Override
        public void handle(ActionResult event) {
            Value vName = event.getParameter("name");
            if (vName != null) {
                String name = vName.getString();
                watchNode.setDisplayName(name);
            } else {
                watchNode.setDisplayName(null);
            }
            setPathValue();
        }
    }

    static {
        LOGGER = LoggerFactory.getLogger(Watch.class);
    }
}
