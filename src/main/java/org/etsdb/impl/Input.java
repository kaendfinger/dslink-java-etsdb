package org.etsdb.impl;

import java.io.IOException;

public interface Input {
    int read() throws IOException;

    int read(byte[] b, int off, int len) throws IOException;
}
