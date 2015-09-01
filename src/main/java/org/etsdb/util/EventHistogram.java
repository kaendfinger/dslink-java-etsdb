package org.etsdb.util;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class EventHistogram {

    private final int bucketSize;
    private final int[] buckets;
    private final AtomicLong lastTime = new AtomicLong();

    private int position;

    public EventHistogram(int bucketSize, int buckets) {
        this.bucketSize = bucketSize;
        this.buckets = new int[buckets];
        this.position = 0;
        this.lastTime.set(System.currentTimeMillis() / bucketSize);
    }

    public static void main(String[] args)
            throws Exception {
        Random random = new Random();
        EventHistogram ec = new EventHistogram(1000, 60);
        for (int i = 0; i < 1000; i++) {
            ec.hit();
            Thread.sleep(random.nextInt(100));
        }
        System.out.println(Arrays.toString(ec.getEventCounts()));
    }

    public void hit() {
        update();
        this.buckets[this.position] += 1;
    }

    public int[] getEventCounts() {
        return getEventCounts(true);
    }

    private int[] getEventCounts(boolean update) {
        if (update) {
            update();
        }

        int[] result = new int[this.buckets.length];
        int pos = this.position % this.buckets.length + 1;

        System.arraycopy(this.buckets, pos, result, 0, this.buckets.length - pos);
        System.arraycopy(this.buckets, 0, result, this.buckets.length - pos, pos);

        return result;
    }

    private void update() {
        long thisTime = System.currentTimeMillis() / this.bucketSize;
        int diff = (int) (thisTime - this.lastTime.getAndSet(thisTime));
        while (diff > 0) {
            this.position = ((this.position + 1) % this.buckets.length);
            this.buckets[this.position] = 0;
            diff--;
        }
    }
}
