package com.my.sibyl.itemsets.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author abykovsky
 * @since 2/1/15
 */
public enum Measure {

    COUNT,
    SUPPORT,
    CONFIDENCE,
    LIFT;

    private static final List<String> VALUES = new ArrayList<>();

    static {
        for (Measure measure : Measure.values()) {
            VALUES.add(measure.toString().toLowerCase());
        }
    }

    public static List<String> stringValues() {
        return VALUES;
    }
}
