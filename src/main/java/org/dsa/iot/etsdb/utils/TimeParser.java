package org.dsa.iot.etsdb.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author Samuel Grenier
 */
public class TimeParser {

    private static final ThreadLocal<DateFormat> FORMAT;

    public static long parse(String time) {
        try {
            return FORMAT.get().parse(time).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static String parse(long time) {
        return FORMAT.get().format(new Date(time));
    }

    static {
        FORMAT = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                return sdf;
            }
        };
    }
}
