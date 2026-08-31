package com.loomq.config;

import java.util.Locale;
import java.util.Properties;

/**
 * Shared helpers for configuration records.
 */
public final class ConfigSupport {

    private ConfigSupport() {
    }

    public static String firstNonBlank(Properties props, String... keys) {
        if (props == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            if (key == null || key.isBlank()) {
                continue;
            }
            String value = props.getProperty(key);
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }

    public static String string(Properties props, String defaultValue, String... keys) {
        String value = firstNonBlank(props, keys);
        return value != null ? value : defaultValue;
    }

    public static int intValue(Properties props, int defaultValue, String... keys) {
        String value = firstNonBlank(props, keys);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value: " + value, e);
        }
    }

    public static long longValue(Properties props, long defaultValue, String... keys) {
        String value = firstNonBlank(props, keys);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long value: " + value, e);
        }
    }

    public static double doubleValue(Properties props, double defaultValue, String... keys) {
        String value = firstNonBlank(props, keys);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid double value: " + value, e);
        }
    }

    public static boolean booleanValue(Properties props, boolean defaultValue, String... keys) {
        String value = firstNonBlank(props, keys);
        if (value == null) {
            return defaultValue;
        }

        return switch (value.toLowerCase(Locale.ROOT)) {
            case "true", "1", "yes", "on" -> true;
            case "false", "0", "no", "off" -> false;
            default -> throw new IllegalArgumentException("Invalid boolean value: " + value);
        };
    }
}
