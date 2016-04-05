package org.etsdb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public abstract class AbstractProperties {
    private static final Pattern PATTERN_ENV = Pattern.compile("(\\$\\{env:(.+?)\\})");
    private static final Pattern PATTERN_PROP = Pattern.compile("(\\$\\{prop:(.+?)\\})");
    private static final Pattern PATTERN_BS = Pattern.compile("\\\\");
    private static final Pattern PATTERN_DOL = Pattern.compile("\\$");

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String description;

    protected AbstractProperties() {
        this("unnamed");
    }

    public AbstractProperties(String description) {
        this.description = description;
    }

    public String getString(String key) {
        String s = getStringImpl(key);
        if (s == null) {
            return null;
        }
        Matcher matcher = PATTERN_ENV.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String pkey = matcher.group(2);
            matcher.appendReplacement(sb, escape(System.getenv(pkey)));
        }
        matcher.appendTail(sb);

        matcher = PATTERN_PROP.matcher(sb.toString());
        sb = new StringBuffer();
        while (matcher.find()) {
            String pkey = matcher.group(2);
            matcher.appendReplacement(sb, escape(System.getProperty(pkey)));
        }
        matcher.appendTail(sb);

        return sb.toString();
    }

    private String escape(String s) {
        if (s == null) {
            return "";
        }
        String escaped;
        escaped = PATTERN_BS.matcher(s).replaceAll("\\\\\\\\");
        escaped = PATTERN_DOL.matcher(escaped).replaceAll("\\\\\\$");

        return escaped;
    }

    protected abstract String getStringImpl(String paramString);

    public int getInt(String key, int defaultValue) {
        String value = getString(key);

        if ("".equals(value)) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            String loggingMessage = "(%s) Can't parse int from properties key: %s, value=%s";
            logger.warn(String.format(loggingMessage, this.description, key, value));
        }
        return defaultValue;
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = getString(key);
        if ("".equals(value)) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        String loggingMessage = "(%s) Can't parse boolean from properties key: %s, value=%s";
        logger.warn(String.format(loggingMessage, this.description, key, value));
        return defaultValue;
    }
}
