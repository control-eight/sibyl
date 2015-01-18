package com.my.sibyl.itemsets;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author abykovsky
 * @since 12/2/14
 */
public class MemoryTest {

    private static Long[] ARR = new Long[500000];

    //private static Map<Long, Long> MAP = new HashMap<>();

    private static Node<Long, Long>[] ARR2 = new Node[500000];

    public static void main(String[] args) throws InterruptedException, ParseException {
        for (int i = 0; i < ARR.length; i++) {
            ARR[i] = (long)i;
        }
        for (int i = 0; i < ARR2.length; i++) {
            ARR2[i] = new Node<>((long) i, (long) i);
        }

        SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yy");
        System.out.println(format.format(new Date()));
        System.out.println(format.parse("13-JUL-14"));

        Thread.sleep(50000000);
    }

    static class Node<K,V> {
        final int hash;
        final K key;
        V value;
        Node<K,V> next;

        Node(K key, V value) {
            this.key = key;
            this.hash = this.key.hashCode();
            this.value = value;
        }
    }
}
