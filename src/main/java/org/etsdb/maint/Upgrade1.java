package org.etsdb.maint;

import java.io.File;

public class Upgrade1 extends BaseUpgrade {
    @Override
    void upgrade() throws Exception {
        // Convert negative shard ids to positive.
        File[] base = db.getBaseDir().listFiles();
        if (base != null) {
            for (File sub : base) {
                if (sub.isDirectory()) {
                    File[] subs = sub.listFiles();
                    if (subs != null) {
                        for (File series : subs) {
                            if (series.isDirectory())
                                db.renameSeries(series, series.getName());
                        }
                    }
                }
            }
        }
    }

    @Override
    int nextVersion() {
        return 2;
    }
}
