package org.etsdb.impl;

import java.util.Comparator;

public class Sorter<T> {
    private final Comparator<T> comparator;
    private Sortable<T> head;

    public Sorter(Comparator<T> comparator) {
        this.comparator = comparator;
    }

    public void add(T value) {
        add(new Sortable<>(value));
    }

    public void add(Sortable<T> e) {
        if (head == null)
            head = e;
        else
            head = head.add(e, comparator);
    }

    public Sortable<T> pop() {
        if (head == null)
            return null;

        Sortable<T> s = head;
        head = s.next;
        s.next = null;
        return s;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (head != null) {
            sb.append(head.value);
            Sortable<T> s = head;
            while ((s = s.next) != null)
                sb.append(", ").append(s.value);
        }
        return sb.toString();
    }

    public static class Sortable<T> {
        Sortable<T> next;
        T value;

        public Sortable(T value) {
            this.value = value;
        }

        Sortable<T> add(Sortable<T> that, Comparator<T> comparator) {
            int comp = comparator.compare(value, that.value);
            if (comp > 0) {
                that.next = this;
                return that;
            }

            if (next == null)
                next = that;
            else
                next = next.add(that, comparator);
            return this;
        }

        public T getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "" + value;
        }
    }
}
