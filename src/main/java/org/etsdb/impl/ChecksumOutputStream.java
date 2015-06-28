package org.etsdb.impl;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

class ChecksumOutputStream extends OutputStream {
    private final OutputStream delegate;

    private byte sum;

    ChecksumOutputStream(OutputStream delegate) {
        if (delegate instanceof BufferedOutputStream)
            this.delegate = delegate;
        else
            // TODO this buffer can be tested with different sizes. Default is 8K.
            this.delegate = new BufferedOutputStream(delegate, 128000); // 128 Kb
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

    void write(ByteBuffer bb) throws IOException {
        while (bb.remaining() > 0)
            write(bb.get());
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
