package org.etsdb.maint;

import org.etsdb.impl.DatabaseImpl;

abstract public class BaseUpgrade {
    protected DatabaseImpl<?> db;

    public void setDb(DatabaseImpl<?> db) {
        this.db = db;
    }

    abstract int nextVersion();

    abstract void upgrade() throws Exception;
}
