package org.etsdb.impl;

import java.io.IOException;

/**
 * Required interface because we need to do checksums on records read from both input streams and random access files.
 *
 * @author Matthew
 */
interface ChecksumInput extends Input {
    boolean checkSum() throws IOException;

    boolean isEof();
}
