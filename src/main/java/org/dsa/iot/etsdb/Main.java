package org.dsa.iot.etsdb;

import org.dsa.iot.dslink.DSLink;
import org.dsa.iot.dslink.DSLinkFactory;
import org.dsa.iot.dslink.DSLinkHandler;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeManager;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.*;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.etsdb.actions.CreateWatchGroupAction;
import org.dsa.iot.etsdb.actions.GetHistory;
import org.dsa.iot.etsdb.etsdb.WatchGroup;
import org.dsa.iot.etsdb.utils.LinkPair;
import org.dsa.iot.etsdb.etsdb.ValueSerializer;
import org.etsdb.Database;
import org.etsdb.DatabaseFactory;
import org.etsdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Samuel Grenier
 */
public class Main extends DSLinkHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private final LinkPair pair = new LinkPair();
    private final Database<Value> database;

    private Main(Database<Value> database) {
        this.database = database;
    }

    @Override
    public void onResponderInitialized(DSLink link) {
        pair.setResponder(link);
        NodeManager manager = link.getNodeManager();
        Node node = manager.getSuperRoot();
        initResponderActions(node);
        DatabaseStats.init(database, node);

        // DEBUG comment
        //node = manager.getNode("/defs/profile/getHistory_", true).getNode();
        //GetHistory.initProfile(node);
        //node = manager.getSuperRoot().getChild("defs");
        //node.setSerializable(false);
    }

    @Override
    public void onRequesterConnected(DSLink link) {
        LOGGER.info("Connected");
        pair.setRequester(link);
        subscribeWatchGroups();
    }

    private void subscribeWatchGroups() {
        NodeManager resp = pair.getResponder().getNodeManager();
        Map<String, Node> children = resp.getSuperRoot().getChildren();
        if (children == null) {
            return;
        }

        for (Node group : children.values()) {
            if (group.getAction() != null
                    || "defs".equals(group.getName())
                    || group.getChild("watches") == null) {
                continue;
            }

            WatchGroup wg = new WatchGroup(database, group, pair);
            wg.init(true);
        }
    }

    private void initResponderActions(Node parent) {
        Map<String, Node> children = parent.getChildren();
        if (children != null) {
            for (Node node : children.values()) {
                WatchGroup group = new WatchGroup(database, node, pair);
                group.init(false);
            }
        }

        NodeBuilder builder = parent.createChild("createWatchGroup");
        builder.setAction(CreateWatchGroupAction.make(database, parent, pair));
        builder.getChild().setSerializable(false);
        builder.build();
    }

    public static void main(String[] args) {
        File dir = new File("db");
        Serializer<Value> ser = new ValueSerializer();
        Database<Value> db = DatabaseFactory.createDatabase(dir, ser);
        DSLinkFactory.startDual("etsdb", args, new Main(db));
    }
}
