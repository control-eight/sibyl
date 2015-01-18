package com.my.sibyl.itemsets;

import org.apache.commons.lang3.tuple.MutablePair;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/11/15
 */
public class TestTest {

    public static void main(String[] args) {
        String string = "X1044349630";
        string = string.replaceAll("[^\\d]*", "");
        System.out.println(string);


        List<Long> list = Arrays.asList(
                1421001530462l,
                1421001531418l,
                1421001534278l,
                1421001535231l,
                1421001536190l,
                1421001529509l,
                1421001532373l,
                1421001011623l,
                1421001012575l,
                1421001528550l,
                1421001537145l,
                1421001527593l,
                1421001533325l);

        MutablePair<Long, Long> tuple = new MutablePair<>(Long.MAX_VALUE, Long.MIN_VALUE);
        for (Long value : list) {
            if(value < tuple.getLeft()) tuple.setLeft(value);
            if(value > tuple.getRight()) tuple.setRight(value);
        }
        System.out.println(tuple.getLeft() + " " + tuple.getRight());
        System.out.println(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date(tuple.getLeft())));
        System.out.println(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date(tuple.getRight())));

        System.out.println(System.nanoTime());
    }
}
