package org.dsa.iot.etsdb;

import org.dsa.iot.etsdb.db.DbProvider;
import org.dsa.iot.historian.Historian;
import org.dsa.iot.historian.database.DatabaseProvider;

/**
 * @author Samuel Grenier
 */
public class Main extends Historian {

    public Main(DatabaseProvider provider) {
        super(provider);
    }

    public static void main(String[] args) {
        Main main = new Main(new DbProvider());
        main.start("etsdb", args);
    }
}
