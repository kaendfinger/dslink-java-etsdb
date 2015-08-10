package org.dsa.iot.etsdb;

import org.dsa.iot.etsdb.db.DbProvider;
import org.dsa.iot.historian.Historian;
import org.dsa.iot.historian.database.DatabaseProvider;

/**
 * @author Samuel Grenier
 */
public class Main extends Historian {

    private final DbProvider provider;

    public Main() {
        this.provider = new DbProvider();
    }

    @Override
    public DatabaseProvider createProvider() {
        return provider;
    }

    @Override
    public void stop() {
        super.stop();
        provider.stop();
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.start(args);
    }
}
