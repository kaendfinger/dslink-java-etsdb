package org.dsa.iot.etsdb.db;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.EditorType;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.database.DatabaseProvider;
import org.dsa.iot.historian.database.Watch;
import org.dsa.iot.historian.utils.TimeParser;
import org.etsdb.impl.DatabaseImpl;
import org.vertx.java.core.Handler;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * @author Samuel Grenier
 */
public class DbProvider extends DatabaseProvider {

    private final DbPurger purger = new DbPurger();

    public DbProvider() {
        purger.setupPurger();
    }

    public DbPurger getPurger() {
        return purger;
    }

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

    @Override
    public void onWatchAdded(final Watch watch) {
        final Node node = watch.getNode();
        final Database database = watch.getGroup().getDb();
        final Permission perm = database.getProvider().dbPermission();
        {
            NodeBuilder b = node.createChild("unsubPurge");
            b.setDisplayName("Unsubscribe and Purge");
            Action a = new Action(perm, new Handler<ActionResult>() {
                @Override
                public void handle(ActionResult event) {
                    watch.unsubscribe();

                    String path = node.getName();
                    path = path.replaceAll("%2F", "/");
                    DatabaseImpl<Value> db = ((Db) database).getDb();
                    db.deleteSeries(path);
                }
            });
            b.setAction(a);
            b.build();
        }
        {
            NodeBuilder b = node.createChild("purge");
            b.setDisplayName("Purge");
            Action a = new Action(perm, new Handler<ActionResult>() {
                @Override
                public void handle(ActionResult event) {
                    long fromTs = Long.MIN_VALUE;
                    long toTs = Long.MAX_VALUE;

                    Value vTR = event.getParameter("Timerange");
                    if (vTR != null) {
                        String[] split = vTR.getString().split("/");
                        fromTs = TimeParser.parse(split[0]);
                        toTs = TimeParser.parse(split[1]);
                    }

                    String path = node.getName();
                    path = path.replaceAll("%2F", "/");
                    DatabaseImpl<Value> db = ((Db) database).getDb();
                    db.delete(path, fromTs, toTs);
                }
            });
            {
                Parameter p = new Parameter("Timerange", ValueType.STRING);
                {
                    String desc = "The range for which to purge data";
                    p.setDescription(desc);
                }
                p.setEditorType(EditorType.DATE_RANGE);
                a.addParameter(p);
            }
            b.setAction(a);
            b.build();
        }
    }
}
