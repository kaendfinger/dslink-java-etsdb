package org.dsa.iot.etsdb.actions;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.*;
import org.dsa.iot.dslink.node.actions.table.Row;
import org.dsa.iot.dslink.node.actions.table.Table;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.historian.stats.Interval;
import org.dsa.iot.historian.utils.TimeParser;
import org.etsdb.Database;
import org.etsdb.QueryCallback;
import org.vertx.java.core.Handler;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Samuel Grenier
 */
public class GetHistory implements Handler<ActionResult> {

    private final Database<Value> db;
    private final int subStringPos;

    private GetHistory(Node data, Database<Value> db) {
        this.subStringPos = data.getPath().length();
        this.db = db;
    }

    @Override
    public void handle(ActionResult event) {
        String range = event.getParameter("Timerange").getString();
        String[] split = range.split("/");

        final String sFrom = split[0];
        final String sTo = split[1];

        long from = TimeParser.parse(sFrom);
        long to = TimeParser.parse(sTo);

        String path;
        {
            Node node = event.getNode().getParent();
            path = node.getPath();
            path = path.substring(subStringPos);
        }

        final String sInterval = event.getParameter("Interval").getString();
        final String sRollup = event.getParameter("Rollup").getString();
        final Interval interval = Interval.parse(sInterval, sRollup);

        final Table table = event.getTable();
        db.query(path, from, to, new QueryCallback<Value>() {
            @Override
            public void sample(String seriesId, long ts, Value value) {
                Row row;
                if (interval == null) {
                    row = new Row();
                    row.addValue(new Value(TimeParser.parse(ts)));
                    row.addValue(value);
                } else {
                    row = interval.getRowUpdate(value, ts);
                }

                if (row != null) {
                    table.addRow(row);
                }
            }
        });
    }

    public static Action make(Node data, Node parent, Database<Value> db) {
        Action a =  new Action(Permission.READ, new GetHistory(data, db));
        initProfile(parent, a);
        //a.setHidden(true);
        return a;
    }

    public static void initProfile(Node node) {
        Action act = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
            }
        });

        initProfile(node, act);
    }

    private static void initProfile(Node node, Action act) {
        {
            Parameter param = new Parameter("Timerange", ValueType.STRING);
            param.setEditorType(EditorType.DATE_RANGE);
            act.addParameter(param);
        }

        {
            Value def = new Value("none");
            Parameter param = new Parameter("Interval", ValueType.STRING, def);
            act.addParameter(param);
        }

        {
            Set<String> enums = new LinkedHashSet<>();
            enums.add("none");
            enums.add("avg");
            enums.add("min");
            enums.add("max");
            enums.add("sum");
            enums.add("first");
            enums.add("last");
            enums.add("count");
            enums.add("delta");
            ValueType e = ValueType.makeEnum(enums);
            Parameter param = new Parameter("Rollup", e);
            act.addParameter(param);
        }

        {
            Parameter param = new Parameter("timestamp", ValueType.TIME);
            act.addResult(param);
        }

        {
            Parameter param = new Parameter("value", ValueType.DYNAMIC);
            act.addResult(param);
        }

        act.setResultType(ResultType.TABLE);
        node.setAction(act);
    }
}
