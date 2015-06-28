package org.dsa.iot.etsdb.actions;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.etsdb.etsdb.WatchGroup;
import org.dsa.iot.etsdb.utils.LinkPair;
import org.etsdb.Database;
import org.vertx.java.core.Handler;

/**
 * @author Samuel Grenier
 */
public class CreateWatchGroupAction implements Handler<ActionResult> {

    private final Database<Value> db;
    private final Node parent;
    private final LinkPair pair;

    private CreateWatchGroupAction(Database<Value> db,
                                   Node parent, LinkPair pair) {
        this.parent = parent;
        this.pair = pair;
        this.db = db;
    }

    @Override
    public synchronized void handle(ActionResult event) {
        String name = event.getParameter("name", ValueType.STRING).getString();
        if (parent.getChild(name) != null) {
            throw new RuntimeException("Watch group already exists");
        }

        final Node node = parent.createChild(name).build();
        WatchGroup group = new WatchGroup(db, node, pair);
        group.init(true);
    }

    public static Action make(Database<Value> db, Node parent, LinkPair pair) {
        CreateWatchGroupAction cwga = new CreateWatchGroupAction(db, parent, pair);
        Action a = new Action(Permission.READ, cwga);
        a.addParameter(new Parameter("name", ValueType.STRING));
        return a;
    }
}
