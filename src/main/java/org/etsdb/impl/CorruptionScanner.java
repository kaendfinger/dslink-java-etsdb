package org.etsdb.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class CorruptionScanner {

    private static final Logger logger = LoggerFactory.getLogger(CorruptionScanner.class.getName());

    private final DatabaseImpl<?> db;

    CorruptionScanner(DatabaseImpl<?> db) {
        this.db = db;
    }

    public static void main(String[] args) throws IOException {
        CorruptionScanner scanner = new CorruptionScanner(null);
        File data = new File("corruption/1175.data");
        long time = System.currentTimeMillis();
        scanner.checkFile(data);
        System.out.println("Time: " + (System.currentTimeMillis() - time));

        for (int i = 101; i < 117; i++) {
            data = new File("corruption/" + i + ".data");
            scanner.checkFile(data);
        }
    }

    void scan() throws IOException {
        File[] subdirs = db.getBaseDir().listFiles();
        if (subdirs != null) {
            for (File subdir : subdirs) {
                File[] seriesDirs = subdir.listFiles();
                if (seriesDirs != null) {
                    for (File seriesDir : seriesDirs) {
                        checkSeriesDir(seriesDir);
                    }
                }
            }
        }
    }

    private void checkSeriesDir(File seriesDir) throws IOException {
        String seriesId = seriesDir.getName();

        // temp files.
        for (File temp : getFiles(seriesDir, ".temp")) {
            long shardId = Utils.getShardId(temp.getName(), 10);
            File data = new File(seriesDir, shardId + ".data");
            File meta = new File(seriesDir, shardId + ".meta");

            if (data.exists()) {
                // If the data file exists, then just delete the file
                logger.warn("Found temp file " + temp + " with existing data file. Deleting.");
                Utils.deleteWithRetry(temp);
            } else if (meta.exists()) {
                // If the meta file exists, then rename the temp file to data, and delete the meta file so that it gets
                // recreated.
                logger.warn("Found temp file " + temp + " without data but with meta file. Moving.");
                Utils.renameWithRetry(temp, data);
                Utils.deleteWithRetry(meta);
            } else if (temp.length() > 0) {
                // A lonely temp file, but with content. Rename to data and see wht the corruption check has to say.
                logger.warn("Found temp file " + temp + " without data or meta file, with content. Moving.");
                Utils.deleteWithRetry(temp);
            } else {
                // Otherwise, just delete the temp file.
                logger.warn("Found temp file " + temp + " without data, meta file, or content. Deleting.");
                Utils.deleteWithRetry(temp);
            }
        }

        // Ensure there is a meta file for every data file and vice versa.
        List<File> datas = getFiles(seriesDir, ".data");
        List<File> metas = getFiles(seriesDir, ".meta");

        for (File data : datas) {
            long shardId = Utils.getShardId(data.getName());
            boolean found = false;
            for (int i = metas.size() - 1; i >= 0; i--) {
                File meta = metas.get(i);
                if (Utils.getShardId(meta.getName()) == shardId) {
                    metas.remove(i);
                    found = true;
                    break;
                }
            }

            if (!found) {
                logger.warn("Data file without meta file in series " + seriesId + ", shard " + shardId + ".");
                // Don't need to recreate the meta file here. The
                //                DataShard shard = new DataShard(seriesDir, seriesId, shardId);
                //                shard.close();
            }
        }

        // If there are any files left in the meta list, then they should just be deleted.
        for (File meta : metas) {
            logger.warn("Meta file without data file at " + meta + ". Deleting file");
            Utils.deleteWithRetry(meta);
        }

        // Check data files for corruption.
        for (File data : datas) {
            checkFile(data);
        }
    }

    private List<File> getFiles(File dir, String suffix) {
        List<File> result = new ArrayList<>();
        File[] files = dir.listFiles(new SuffixFilter(suffix));
        if (files != null) {
            Collections.addAll(result, files);
        }
        return result;
    }

    private void checkFile(File data) throws IOException {
        long position = 0;
        // Start a detect/fix loop.
        while (true) {
            position = findCorruption(data, position);
            if (position == -1) {
                break;
            }

            // If any corruption was found, delete the meta file so that it gets recreated.
            Utils.deleteWithRetry(new File(data.getParent(), Utils.getShardId(data.getName()) + ".meta"));

            logger.warn("Corruption detected in " + data + " at position " + position);
            fixCorruption(data, position);
        }
    }

    private long findCorruption(File data, long startPosition) throws IOException {
        ChecksumInputStream in = null;
        try {
            ScanInfo scanInfo = new ScanInfo();
            in = new ChecksumInputStream(data);

            if (startPosition > 0) {
                Utils.skip(in, startPosition);
            }

            while (true) {
                long position = in.position();
                if (!checkRow(in, scanInfo)) {
                    return position;
                }
                if (scanInfo.isEof()) {
                    break;
                }
            }
        } finally {
            Utils.closeQuietly(in);
        }

        return -1;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void fixCorruption(File data, long badRowposition) throws IOException {
        ChecksumInputStream in = null;
        try {
            ScanInfo scanInfo = new ScanInfo();
            in = new ChecksumInputStream(data);
            Utils.skip(in, badRowposition);

            while (true) {
                in.read();
                in.mark(Utils.MAX_DATA_LENGTH + 10);
                long position = in.position();
                if (!checkRow(in, scanInfo)) {
                    // Bad row. Keep looking for a good one.
                    in.reset();
                    continue;
                } else if (scanInfo.isEof()) {
                    // We reached the EOF before finding a good row. Splice off the end of the file.
                    Utils.closeQuietly(in);
                    cut(data, badRowposition, position);
                    break;
                }

                // Now, look for another 2 to add confidence that it's for real.
                if (!checkRow(in, scanInfo)) {
                    // Bad row. The previous "good" one was probably just a fluke. Keep looking.
                    in.reset();
                    continue;
                } else if (scanInfo.isEof()) {
                    // We reached the EOF before finding a second good row. We'll assume the row is good and cut out
                    // the bad bit.
                    Utils.closeQuietly(in);
                    cut(data, badRowposition, position);
                    break;
                }

                // Found 2 good ones (or we're at EOF). Looking good...
                if (!checkRow(in, scanInfo)) {
                    // Oy. It's a stretch for there to be two false positives in a row, but we're going to call it a 
                    // fluke and keep looking.
                    in.reset();
                    continue;
                }

                // Ok, good enough, or we're at the EOF. Either way, cut out the bad row and call it fixed.
                Utils.closeQuietly(in);
                cut(data, badRowposition, position);
                break;
            }
        } finally {
            Utils.closeQuietly(in);
        }
    }

    private boolean checkRow(ChecksumInputStream in, ScanInfo scanInfo) throws IOException {
        try {
            DataShard._readSample(in, scanInfo);
        } catch (BadRowException e) {
            return false;
        }

        if (scanInfo.isEof()) // No row was read because we normally reached the EOF.
        {
            return true;
        }

        // ??? Check that the record's ts is greater than 0, greater than the last, and less than the shard max.
        // Verify the checksum.
        return in.checkSum();

    }

    /**
     * Deletes bytes in a file
     * <p>
     *
     * @param data the data file to splice
     * @param from the inclusive position to delete from
     * @param to   the exclusive position to delete to
     * @throws IOException
     */
    private void cut(File data, long from, long to) throws IOException {
        File temp = new File(data.getParentFile(), "temp");
        FileInputStream in = null;
        FileOutputStream out = null;

        logger.warn("Cutting corrupt data in " + data + " at " + from + ", length " + (to - from));

        try {
            in = new FileInputStream(data);
            out = new FileOutputStream(temp);
            byte[] buf = new byte[8192];

            // Write to the from position.
            copy(in, out, from, buf);

            // Skip the difference in the input stream.
            Utils.skip(in, to - from);

            // Write the remainder of the input stream.
            copy(in, out, data.length() - to, buf);
        } finally {
            Utils.closeQuietly(in);
            Utils.closeQuietly(out);
        }

        Utils.deleteWithRetry(data);
        Utils.renameWithRetry(temp, data);
    }

    private void copy(InputStream in, OutputStream out, long length, byte[] buf) throws IOException {
        while (length > 0) {
            int chunk = buf.length;
            if (length < buf.length) {
                chunk = (int) length;
            }
            int read = in.read(buf, 0, chunk);
            if (read == -1) {
                break;
            }
            out.write(buf, 0, read);
            length -= read;
        }
    }

    static class SuffixFilter implements FilenameFilter {

        private final String suffix;

        SuffixFilter(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(suffix);
        }
    }
}
