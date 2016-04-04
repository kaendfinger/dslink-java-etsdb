package org.etsdb.maint;

import org.etsdb.EtsdbException;
import org.etsdb.impl.DatabaseImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBUpgrade {
    private static final Logger LOGGER = LoggerFactory.getLogger(DBUpgrade.class.getName());

    private final DatabaseImpl<?> db;

    public DBUpgrade(DatabaseImpl<?> db) {
        this.db = db;
    }

    public void checkForUpgrade() {
        // Get the current database version.
        int currentVersion = db.getProperties().getInt("version", 0);

        // There may be multiple upgrades to run, so start a loop.
        while (true) {
            // Convert the schema version to the class name convention. This simply means replacing dots with
            // underscores and prefixing 'Upgrade' and this package.
            String upgradeClassname = "org.etsdb.maint.Upgrade" + currentVersion;

            // See if there is a class with this name.
            Class<?> clazz = null;
            BaseUpgrade upgrade = null;
            try {
                clazz = Class.forName(upgradeClassname);
            } catch (ClassNotFoundException e) {
                // no op
            }

            if (clazz != null) {
                try {
                    upgrade = (BaseUpgrade) clazz.newInstance();
                } catch (Exception e) {
                    // Should never happen so wrap in a runtime and rethrow.
                    throw new EtsdbException(e);
                }
            }

            if (upgrade == null) {
                if (currentVersion != DatabaseImpl.VERSION) {
                    String errorMessage = String
                            .format("The code version %d does not match the database version %d", DatabaseImpl.VERSION, currentVersion);
                    throw new EtsdbException(errorMessage);
                }
                break;
            }

            try {
                LOGGER.info("Upgrading from " + currentVersion + " to " + upgrade.nextVersion());

                upgrade.setDb(db);
                upgrade.upgrade();
                currentVersion = upgrade.nextVersion();
                db.getProperties().setInt("version", currentVersion);
            } catch (Exception e) {
                throw new EtsdbException(e);
            }
        }
    }
}
