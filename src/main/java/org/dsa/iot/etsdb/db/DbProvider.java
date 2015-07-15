package org.dsa.iot.etsdb.db;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.database.DatabaseProvider;
import org.vertx.java.core.Handler;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * @author Samuel Grenier
 */
public class DbProvider extends DatabaseProvider {

    @Override
    public Action createDbAction(Permission perm) {
        Action act = new Action(perm,
                        new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Value vPath = event.getParameter("Path", ValueType.STRING);
                String path = vPath.getString();

                NodeBuilder builder;
                try {
                    String name = URLEncoder.encode(path, "UTF-8");
                    builder = createDbNode(name, event);
                    builder.setConfig("path", new Value(path));

                    Value vName = event.getParameter("Name");
                    if (vName != null) {
                        builder.setDisplayName(vName.getString());
                    }
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
                createAndInitDb(builder.build());
            }
        });
        {
            Parameter p = new Parameter("Name", ValueType.STRING);
            String desc = "Name of the database\n";
            desc += "If name is not provided, the path will be used for the name";
            p.setDescription(desc);
            act.addParameter(p);

            Value def = new Value("db");
            p = new Parameter("Path", ValueType.STRING, def);
            desc = "Absolute or relative path to the database\n";
            desc += "If the database does not exist, it will be created";
            p.setDescription(desc);
            act.addParameter(p);
        }
        return act;
    }

    @Override
    public Database createDb(Node node) {
        String path = node.getConfig("path").getString();
        return new Db(node.getDisplayName(), path, this);
    }

    @Override
    public Permission dbPermission() {
        return Permission.CONFIG;
    }
}
