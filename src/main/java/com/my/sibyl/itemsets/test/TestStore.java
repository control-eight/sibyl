package com.my.sibyl.itemsets.test;

import com.google.common.primitives.UnsignedBytes;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

    public static void main(String[] args) {
        List<byte[]> arrayList = Arrays.asList(Bytes.toBytes(10l), Bytes.toBytes(1l), Bytes.toBytes(7l), Bytes.toBytes(11l));
        Collections.sort(arrayList, UnsignedBytes.lexicographicalComparator());
        List<Long> arrayListLong = new ArrayList<>();
        for (byte[] bytes : arrayList) {
            arrayListLong.add(Bytes.toLong(bytes));
        }
        System.out.println(arrayListLong);
    }
}
