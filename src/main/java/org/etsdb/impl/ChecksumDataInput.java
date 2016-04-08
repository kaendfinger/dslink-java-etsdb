package org.etsdb.impl;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;

class ChecksumDataInput implements ChecksumInput, Closeable {
    private final RandomAccessFile delegate;

    private byte sum;
    private boolean eof;

    ChecksumDataInput(RandomAccessFile delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean checkSum() throws IOException {
        byte b = (byte) delegate.read();
        boolean match = b == sum;
        sum = 0;
        return match;
    }

    @Override
    public int read() throws IOException {
        if (eof)
            return -1;

        int i = delegate.read();
        sum += (byte) i;
        if (i == -1)
            eof = true;

        return i;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (eof)
            return -1;

        int count = delegate.read(b, off, len);
        if (count == -1) {
            eof = true;
            sum += (byte) -1;
        } else {
            for (int i = 0; i < count; i++)
                sum += b[i + off];
        }

        return count;
    }

    @Override
    public boolean isEof() {
        return eof;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
