package com.my.sibyl.itemsets.analytics;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Map;

/**
 * @author abykovsky
 * @since 12/8/14
 */
public class Some {

    public static void main(String[] args) throws IOException {
        try(Reader in = new FileReader("/Users/abykovsky/recomm_PDP_01.csv")) {
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader().parse(in);

            Multimap<String, String> map = HashMultimap.create();
            for (CSVRecord record : records) {
                map.put(record.get(0), record.get(1));
            }

            Map<String, Collection<String>> aMap = map.asMap();

            for (Map.Entry<String, Collection<String>> entry : aMap.entrySet()) {
                System.out.print("'" + entry.getKey() + "'");
                for (String s : entry.getValue()) {
                    System.out.print(", '" + s + "'");
                }
                System.out.println(" ");
            }
        }
    }
}
