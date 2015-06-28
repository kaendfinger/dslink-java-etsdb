package org.etsdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbConfig {

    private static final Logger logger = LoggerFactory.getLogger(DbConfig.class);

    private boolean addShutdownHook = true;
    private boolean runCorruptionScan = true;
    private boolean deleteEmptyDirs = true;
    private int fileLockCheckInterval = 1000;
    private int flushInterval = 1000 * 60 * 5;

    /**
     * If the shard has not been accessed within this time, it's output streams are closed. The flush process is what
     * enacts this value, so the flush interval should best be equal to or less than this value.
     */
    private int shardStalePeriod = 1000 * 60 * 60;

    private int maxOpenFiles = 500;
    private boolean ignoreBackdates = false;
    private int backdateStartDelay = 5000;

    /**
     * If true, writes will be queued in memory, and written out according to the queue parameters. If false, writes
     * will be written directly to files in process.
     */
    private boolean useWriteQueue = false;

    /**
     * The minimum amount of time in milliseconds shard updates will be cached until they are written to disk by a run
     * of the flush process.
     */
    private int queueExpireMinimum = 150000;

    /**
     * The maximum amount of time in milliseconds shard updates will be cached until they are written to disk by a run
     * of the flush process.
     */
    private int queueExpireMaximum = 450000;

    /**
     * The minimum number of cached writes for a shard before they are written to disk by a run of the flush process.
     * A number between the min and the max is chosen at random (if the values are different) which is used for the
     * actual triggering of the flush.
     */
    private int queueShardQueueSizeMinimum = 25;

    /**
     * The maximum number of cached writes for a shard before they are written to disk by a run of the flush process.
     * A number between the min and the max is chosen at random (if the values are different) which is used for the
     * actual triggering of the flush.
     */
    private int queueShardQueueSizeMaximum = 25;

    /**
     * The maximum number of updates in the queue before they are written to disk by a run of the flush process.
     */
    private int queueMaxQueueSize = 100000;

    /**
     * Because all of the above parameters are only used during a run of the flush process, the actual queue size can
     * exceed the maximum size. The discard queue size is checked with every write, and the given data is discarded
     * if the
     */
    private int queueDiscardQueueSize = 1000000;

    public void validate() throws ConfigException {
        if (fileLockCheckInterval <= 0)
            throw new ConfigException("fileLockCheckInterval must be greater than 0");

        if (flushInterval <= 0)
            throw new ConfigException("flushInterval must be greater than 0");

        if (flushInterval < 300000) {
            logger.warn("Flush interval too low, setting it to 5 minutes (300000 ms)");
            flushInterval = 300000;
        }

        if (shardStalePeriod < 0)
            throw new ConfigException("shardStalePeriod cannot be negative");

        if (backdateStartDelay < 0)
            throw new ConfigException("backdateStartDelay cannot be negative");

        if (useWriteQueue) {
            if (queueExpireMinimum < 0)
                throw new ConfigException("queueExpireMinimum cannot be negative");

            if (queueExpireMaximum < queueExpireMinimum)
                throw new ConfigException("queueExpireMaximum cannot be less than queueExpireMinimum");

            if (queueShardQueueSizeMinimum <= 0)
                throw new ConfigException("queueShardQueueSizeMinimum must be greater than 0 ");

            if (queueMaxQueueSize < queueShardQueueSizeMinimum)
                throw new ConfigException("queueMaxQueueSize must be greater than queueShardQueueSizeMinimum");

            if (queueDiscardQueueSize < queueMaxQueueSize)
                throw new ConfigException("queueDiscardQueueSize must be greater than queueMaxQueueSize");
        }
    }

    public boolean isAddShutdownHook() {
        return addShutdownHook;
    }

    public void setAddShutdownHook(boolean addShutdownHook) {
        this.addShutdownHook = addShutdownHook;
    }

    public boolean isRunCorruptionScan() {
        return runCorruptionScan;
    }

    public void setRunCorruptionScan(boolean runCorruptionScan) {
        this.runCorruptionScan = runCorruptionScan;
    }

    public boolean isDeleteEmptyDirs() {
        return deleteEmptyDirs;
    }

    public void setDeleteEmptyDirs(boolean deleteEmptyDirs) {
        this.deleteEmptyDirs = deleteEmptyDirs;
    }

    public int getFileLockCheckInterval() {
        return fileLockCheckInterval;
    }

    public void setFileLockCheckInterval(int fileLockCheckInterval) {
        this.fileLockCheckInterval = fileLockCheckInterval;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    public int getShardStalePeriod() {
        return shardStalePeriod;
    }

    public void setShardStalePeriod(int shardStalePeriod) {
        this.shardStalePeriod = shardStalePeriod;
    }

    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }

    public void setMaxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
    }

    public boolean isIgnoreBackdates() {
        return ignoreBackdates;
    }

    public void setIgnoreBackdates(boolean ignoreBackdates) {
        this.ignoreBackdates = ignoreBackdates;
    }

    public int getBackdateStartDelay() {
        return backdateStartDelay;
    }

    public void setBackdateStartDelay(int backdateStartDelay) {
        this.backdateStartDelay = backdateStartDelay;
    }

    public boolean isUseWriteQueue() {
        return useWriteQueue;
    }

    public void setUseWriteQueue(boolean useWriteQueue) {
        this.useWriteQueue = useWriteQueue;
    }

    public int getQueueExpireMinimum() {
        return queueExpireMinimum;
    }

    public void setQueueExpireMinimum(int queueExpireMinimum) {
        this.queueExpireMinimum = queueExpireMinimum;
    }

    public int getQueueExpireMaximum() {
        return queueExpireMaximum;
    }

    public void setQueueExpireMaximum(int queueExpireMaximum) {
        this.queueExpireMaximum = queueExpireMaximum;
    }

    public int getQueueShardQueueSizeMinimum() {
        return queueShardQueueSizeMinimum;
    }

    public void setQueueShardQueueSizeMinimum(int queueShardQueueSizeMinimum) {
        this.queueShardQueueSizeMinimum = queueShardQueueSizeMinimum;
    }

    public int getQueueShardQueueSizeMaximum() {
        return queueShardQueueSizeMaximum;
    }

    public void setQueueShardQueueSizeMaximum(int queueShardQueueSizeMaximum) {
        this.queueShardQueueSizeMaximum = queueShardQueueSizeMaximum;
    }

    public int getQueueMaxQueueSize() {
        return queueMaxQueueSize;
    }

    public void setQueueMaxQueueSize(int queueMaxQueueSize) {
        this.queueMaxQueueSize = queueMaxQueueSize;
    }

    public int getQueueDiscardQueueSize() {
        return queueDiscardQueueSize;
    }

    public void setQueueDiscardQueueSize(int queueDiscardQueueSize) {
        this.queueDiscardQueueSize = queueDiscardQueueSize;
    }
}
