package com.my.sibyl.itemsets.analytics;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 1/13/15
 */
public class CalculateRecommendationsMaxSize {

    public static void main(String[] args) throws IOException, ParseException {
        loadData("/Users/abykovsky/export_2013-11-29_2013-12-02.csv");
    }

    private static void loadData(String csvFile) throws IOException, ParseException {

        Map<Long, Set<Long>> map = new HashMap<>();

        Set<Long> all = new HashSet<>();
        try (Reader in = new FileReader(csvFile)) {
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader().parse(in);
            System.out.println("Start load data from \"" + csvFile + "\"");
            for (CSVRecord record : records) {

                List<Long> transactionList = new ArrayList<>();
                for (String s : record.get("PROD_PRCHSD").split("\\|")) {
                    s = s.trim();
                    if (s.length() > 0) {
                        transactionList.add(Long.parseLong(s));
                    }
                }
                all.addAll(transactionList);

                for (int i = 0; i < transactionList.size(); i++) {
                    Long aLong = transactionList.get(i);
                    map.putIfAbsent(aLong, new HashSet<>());
                    ArrayList<Long> other = new ArrayList<>(transactionList);
                    other.remove(i);
                    map.get(aLong).addAll(other);
                }

            }
        }
        System.out.println(all.size());

        int count = 0;
        int count2 = 0;
        int max = Integer.MIN_VALUE;
        for (Map.Entry<Long, Set<Long>> entry : map.entrySet()) {
            count++;
            count2 += entry.getValue().size();
            if(entry.getValue().size() > max) {
                max = entry.getValue().size();
            }
        }
        System.out.println(count + " " + count2 + " " + max);
    }
}
