package org.etsdb.util;

import java.io.File;

public class DirectoryUtils {

    public static DirectoryInfo getSize(File file) {
        DirectoryInfo info = new DirectoryInfo();
        getSizeImpl(info, file);
        return info;
    }

    private static void getSizeImpl(DirectoryInfo info, File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File subFile : files) {
                    getSizeImpl(info, subFile);
                }
            }
        } else {
            info.count += 1;
            info.size += file.length();
        }
    }
}
