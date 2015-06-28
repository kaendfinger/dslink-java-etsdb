package org.etsdb.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamUtils {

    public static void transfer(InputStream in, OutputStream out) throws IOException {
        transfer(in, out, -1L);
    }

    public static void transfer(InputStream in, OutputStream out, long limit)
            throws IOException {
        byte[] buf = new byte['?'];

        long total = 0L;
        int readCount;
        while ((readCount = in.read(buf)) != -1) {
            if ((limit != -1L) &&
                    (total + readCount > limit)) {
                readCount = (int) (limit - total);
            }
            if (readCount > 0) {
                out.write(buf, 0, readCount);
            }
            total += readCount;
            if ((limit != -1L) && (total >= limit)) {
                break;
            }
        }
        out.flush();
    }

}
