package org.dsa.iot.etsdb.tool;

import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.etsdb.serializer.ByteData;
import org.dsa.iot.etsdb.serializer.ValueSerializer;
import org.etsdb.Database;
import org.etsdb.DatabaseFactory;
import org.etsdb.Serializer;
import org.etsdb.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Random;

/**
 * @author Samuel Grenier
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {
        String prop = System.getProperty("etsdb.timestamp_bit_shift", "30");
        int bits = Integer.parseInt(prop);
        LOGGER.info("etsdb.timestamp_bit_shift = {}", bits);
        Utils.setShardBits(bits);

        File file = new File("test_db");
        Serializer<ByteData> ser = new ValueSerializer();
        Database<ByteData> db = DatabaseFactory.createDatabase(file, ser);

        ByteData bd = new ByteData();
        bd.setValue(new Value(RANDOM.nextInt()));
        for (int i = 0; i < 10000000;) {
            db.write("test", System.currentTimeMillis(), bd);
            bd.getValue().set(RANDOM.nextInt());
            if (++i % 100000 == 0) {
                LOGGER.info("Wrote {} rows", i);
            }
        }
        db.close();
        LOGGER.info("Completed writing");
    }

}
