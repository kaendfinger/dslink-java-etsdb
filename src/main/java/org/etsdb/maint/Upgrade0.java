package org.etsdb.maint;

import java.io.File;

@SuppressWarnings("unused")
public class Upgrade0 extends BaseUpgrade {
    @Override
    void upgrade() throws Exception {
        // Convert int series ids to string
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
        return 1;
    }
}
