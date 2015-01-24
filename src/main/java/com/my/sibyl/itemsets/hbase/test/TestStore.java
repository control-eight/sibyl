package com.my.sibyl.itemsets.hbase.test;

import java.util.Map;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public class TestStore {

    private Map<String, Long> test;

    public void set(Map<String, Long> test) {
        System.out.println("Size: " + test.size());
        this.test = test;
    }
}
