package org.dsa.iot.etsdb.utils.atomic;

import org.vertx.java.core.Handler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Samuel Grenier
 */
public class NotifyAtomicInteger {

    private final AtomicInteger aInt = new AtomicInteger();
    private Handler<Integer> handler;

    public void setHandler(Handler<Integer> handler) {
        this.handler = handler;
    }

    public int get() {
        return aInt.get();
    }

    public int addAndGet(int i) {
        return notifyHandler(aInt.addAndGet(i));
    }

    public int incrementAndGet() {
        return notifyHandler(aInt.incrementAndGet());
    }

    public int decrementAndGet() {
        return notifyHandler(aInt.decrementAndGet());
    }

    private int notifyHandler(int i) {
        if (handler != null) {
            handler.handle(i);
        }
        return i;
    }
}
