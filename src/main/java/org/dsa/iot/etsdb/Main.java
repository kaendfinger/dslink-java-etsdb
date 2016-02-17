package org.dsa.iot.etsdb;

import org.dsa.iot.etsdb.db.DbProvider;
import org.dsa.iot.historian.Historian;
import org.dsa.iot.historian.database.DatabaseProvider;
import org.etsdb.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Samuel Grenier
 */
public class Main extends Historian {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private final DbProvider provider;

    public Main() {
        this.provider = new DbProvider();
    }

    @Override
    public DatabaseProvider createProvider() {
        return provider;
    }

    @Override
    public void preInit() {
        super.preInit();

        String prop = System.getProperty("etsdb.timestamp_bit_shift", "30");
        int bits = Integer.parseInt(prop);
        LOGGER.debug("etsdb.timestamp_bit_shift = {}", bits);
        Utils.setShardBits(bits);
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
