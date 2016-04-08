package org.etsdb.impl;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

class ChecksumOutputStream extends OutputStream {
    private final OutputStream delegate;

    private byte sum;

    ChecksumOutputStream(OutputStream delegate) {
        if (delegate instanceof BufferedOutputStream) {
            this.delegate = delegate;
        } else {
            this.delegate = new BufferedOutputStream(delegate, 128000); // 128 Kb
        }
    }

    void writeSum() throws IOException {
        delegate.write(sum);
        sum = 0;
    }

    @Override
    public void write(int b) throws IOException {
        sum += b;
        delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        for (int i = 0; i < len; i++)
            sum += b[i + off];
        delegate.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
