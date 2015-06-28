package org.etsdb.impl;

import java.io.IOException;

public class BadRowException extends IOException {
    private static final long serialVersionUID = 1L;

    public BadRowException(String message) {
        super(message);
    }
}
