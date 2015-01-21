package com.my.sibyl.itemsets.analytics;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.my.sibyl.itemsets.legacy.CandidatesGenerator;
import com.my.sibyl.itemsets.legacy.Container;
import com.my.sibyl.itemsets.legacy.FrequentItemSetsGenerator;
import com.my.sibyl.itemsets.legacy.ItemSetsFilter;
import com.my.sibyl.itemsets.legacy.MutableInteger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 12/3/14
 */
public class CategoryRecommendationCalculation {

    private static final String DELIM = "~\\|~";

    private static Map<Long, Product> productMap = null;

    private static final int LEVEL_THRESHOLD = 0;

    private static final int SLIDING_WINDOW_SIZE = 2_000_000;

    private static final int MAX_BASKET_ITEMS_COUNT = 6;

    private static final ItemSetsFilter itemSetsFilter = new ItemSetsFilter();

    //private static final double MIN_SUPPORT = 0.000006;
    private static final double MIN_SUPPORT = 2;
    private static final double MIN_CONFIDENCE = 0.01;

    private static final int K_TOP = 7;


    private static final FrequentItemSetsGenerator frequentItemSetsGenerator
            = new FrequentItemSetsGenerator(LEVEL_THRESHOLD, itemSetsFilter, SLIDING_WINDOW_SIZE, 2);

    private static final CandidatesGenerator candidatesGenerator
            = new CandidatesGenerator(MIN_SUPPORT, MIN_CONFIDENCE, frequentItemSetsGenerator, itemSetsFilter);

    private static int transactionCount;

    public static void main(String[] args) throws IOException, ParseException {
        productMap = loadProducts("/Users/abykovsky/Downloads/RTD_PRODUCTS_A.dsv");
        itemSetsFilter.setMasterProducts(generate(productMap));
        //System.out.println( productMap.get(1219169l).getCategory());
        loadData("/Users/abykovsky/export_2013-11-29_2013-12-02.csv");
        //loadData("/Users/abykovsky/export_2014-07-07_2014-07-15.csv");
        //checkRTD();
    }

    private static void checkRTD() throws IOException {
        checkRTDCategoryAssoc(loadTransactionItemSet("/Users/abykovsky/export_2014-07-07_2014-07-15.csv"),
                loadRTDRecommendations("/Users/abykovsky/Downloads/RTD_EXTNL_SYS_PROD_REC_A_PDP.dsv")
                //loadRTDRecommendations("/Users/abykovsky/Downloads/RTD_EXTNL_SYS_PROD_REC_A_P9.dsv")
        );
    }

    private static Set<Long> loadTransactionItemSet(String csvFile) throws IOException {
        Set<Long> items = new HashSet<>();
        try (Reader in = new FileReader(csvFile)) {
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader().parse(in);
            System.out.println("Start load data from \"" + csvFile + "\"");
            int i = 0;
            for (CSVRecord record : records) {
                i++;
                List<Double> actualPriceList = new ArrayList<>();
                for (String s : record.get("PROD_ACTL_PRCS").split("\\|")) {
                    s = s.trim();
                    if (s.length() > 0) {
                        actualPriceList.add(Double.parseDouble(s));
                    }
                }

                List<Long> transactionItems = new ArrayList<>();
                for (String s : record.get("PROD_PRCHSD").split("\\|")) {
                    s = s.trim();
                    if (s.length() > 0) {
                        transactionItems.add(Long.parseLong(s));
                    }
                }

                filterFreePrice(transactionItems, actualPriceList);

                if (transactionItems.size() > MAX_BASKET_ITEMS_COUNT) {
                    continue;
                }

                Collections.addAll(items, transactionItems.toArray(new Long[transactionItems.size()]));
            }
            System.out.println("End load data. " + i + " rows were processed");
        }
        return items;
    }

    private static Map<Long, Long> generate(Map<Long, Product> productMap) {
        Map<Long, Long> result = new HashMap<>();
        for (Map.Entry<Long, Product> entry : productMap.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getMasterProductId());
        }
        return result;
    }

    private static void loadData(String csvFile) throws IOException, ParseException {
        Map<String, MutableInteger> categoryCountMap = new HashMap<>();
        Map<String, Map<String, MutableInteger>> categoryCountAssocMap = new HashMap<>();

        int nullCount = 0;
        int rugsCount = 0;
        int rugsSingleCount = 0;
        Map<String, MutableInteger> rugsAssoc = new HashMap<>();
        try (Reader in = new FileReader(csvFile)) {
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader().parse(in);
            System.out.println("Start load data from \"" + csvFile + "\"");
            int all = 0;

            Set<Long> items = new HashSet<>();

            for (CSVRecord record : records) {
                List<Double> actualPriceList = new ArrayList<>();
                for (String s : record.get("PROD_ACTL_PRCS").split("\\|")) {
                    s = s.trim();
                    if (s.length() > 0) {
                        actualPriceList.add(Double.parseDouble(s));
                    }
                }

                List<Long> transactionItems = new ArrayList<>();
                for (String s : record.get("PROD_PRCHSD").split("\\|")) {
                    s = s.trim();
                    if (s.length() > 0) {
                        transactionItems.add(Long.parseLong(s));
                    }
                }

                filterFreePrice(transactionItems, actualPriceList);

                all++;
                if (transactionItems.size() > MAX_BASKET_ITEMS_COUNT) {
                    continue;
                }

                boolean isThereRugs = false;
                for (Long transactionItem : transactionItems) {
                    if(productMap.get(transactionItem) != null) {
                        if ("Rugs".equals(productMap.get(transactionItem).getCategory())) {
                            //System.out.println(transactionItems);
                            //break;
                            rugsCount++;
                            if(transactionItems.size() == 1) rugsSingleCount++;
                            isThereRugs = true;
                        }
                    }
                }
                if(isThereRugs) {
                    for (Long transactionItem : transactionItems) {
                        Product product = productMap.get(transactionItem);
                        if(product != null && !"Rugs".equals(product.getCategory())) {
                            rugsAssoc.putIfAbsent(product.getCategory(), new MutableInteger(0));
                            rugsAssoc.get(product.getCategory()).incrementAndGet();
                        }
                    }
                }

                for (Long transactionItem : transactionItems) {
                    if(productMap.get(transactionItem) == null) nullCount++;
                }

                Collections.addAll(items, transactionItems.toArray(new Long[transactionItems.size()]));

                Map<Set<Long>, Integer> difference = frequentItemSetsGenerator.process(transactionItems);
                candidatesGenerator.process(difference, ++transactionCount);
            }
            System.out.println("rugsCount: " + rugsCount);
            System.out.println("rugsSingleCount: " + rugsSingleCount);
            System.out.println(rugsAssoc);
            System.out.println("nullCount: " + nullCount);
            System.out.println("End load data. " + all + " rows were processed.");
            candidatesGenerator.print();

            int emptyCount = 0;
            for (Long item : items) {
                Product product = productMap.get(item);
                if (product != null) {
                    String category = product.getCategory();
                    categoryCountMap.putIfAbsent(category, new MutableInteger(0));
                    categoryCountMap.get(category).incrementAndGet();
                    categoryCountAssocMap.putIfAbsent(category, new HashMap<>());

                    for (Container container : candidatesGenerator.getTopRecommendations(generateSet(item), K_TOP, transactionCount)) {
                        Product product1 = productMap.get(container.getItem());
                        if (product1 != null) {
                            categoryCountAssocMap.get(category).putIfAbsent(product1.getCategory(), new MutableInteger(0));
                            categoryCountAssocMap.get(category).get(product1.getCategory()).incrementAndGet();
                        }
                    }
                } else {
                    emptyCount++;
                }
            }
            System.out.println(emptyCount);

            List<Map.Entry<String, MutableInteger>> categoryCountList = new ArrayList<>(categoryCountMap.entrySet());
            Collections.sort(categoryCountList, (o1, o2) -> o2.getValue().getValue() - o1.getValue().getValue());

            for (Map.Entry<String, MutableInteger> entry : categoryCountList.subList(0, Math.min(categoryCountList.size(), 50))) {
                List<Map.Entry<String, MutableInteger>> categoryCountAssocList = new ArrayList<>(categoryCountAssocMap
                        .get(entry.getKey()).entrySet());
                Collections.sort(categoryCountAssocList, (o1, o2) -> o2.getValue().getValue() - o1.getValue().getValue());
                System.out.print("\"" + entry.getKey() + "\"=" + entry.getValue() + " => ");
                for (Map.Entry<String, MutableInteger> integerEntry : categoryCountAssocList.subList(0,
                        Math.min(categoryCountAssocList.size(), 20))) {
                    System.out.print("\"" + integerEntry.getKey() + "\"=" + integerEntry.getValue() + ", ");
                }
                System.out.println(" ");
            }
        }
    }

    private static void filterFreePrice(List<Long> transactionItems, List<Double> actualPriceList) {
        for (int i = 0; i < transactionItems.size(); i++) {
            if (actualPriceList.get(i) == 0.0) {
                transactionItems.remove(i);
                i--;
            }
        }
    }

    private static boolean isFreePrice(int transactionItem, List<Double> actualPriceList) {
        return actualPriceList.get(transactionItem) == 0.0;
    }

    public static Map<Long, Product> loadProducts(String csvFile) throws IOException {
        System.out.println("Start load data from \"" + csvFile + "\"");
        Map<Long, Product> result = new HashMap<>();
        try (Reader in = new FileReader(csvFile)) {
            BufferedReader bf = new BufferedReader(in);
            String line;
            int i = -1;
            String[] header = null;
            while ((line = bf.readLine()) != null) {
                i++;
                if (line.trim().length() == 0) continue;
                if (i == 32767 || (i > 32767 && (i - 32767) % 32766 == 0)) continue;

                //71-72
                String[] arr = line.split(DELIM);
                if (i == 1) {
                    header = arr;
                } else if (i > 1) {
                    String str = arr[3].trim();
                    Long masterProductId = null;
                    try {
                        if (!str.isEmpty()) {
                            masterProductId = Long.parseLong(str);
                        }
                    } catch (NumberFormatException ex) {
                        ex.printStackTrace();
                    }
                    Long productId = Long.parseLong(arr[2].trim());

                    Product product = new Product();
                    product.setId(productId);
                    product.setMasterProductId(masterProductId);
                    product.setCategory(arr[14].trim());

                    result.putIfAbsent(productId, product);
                }
            }
            System.out.println("End load data. " + i + " rows were processed");
        }
        return result;
    }

    public static Multimap<Long, Long> loadRTDRecommendations(String csvFile) throws IOException {
        System.out.println("Start load data from \"" + csvFile + "\"");
        Multimap<Long, Long> result = HashMultimap.create();
        try (Reader in = new FileReader(csvFile)) {
            BufferedReader bf = new BufferedReader(in);
            String line;
            int i = -1;
            String[] header = null;
            while ((line = bf.readLine()) != null) {
                i++;
                if(line.trim().length() == 0) continue;
                if(i == 32767 || (i > 32767 && (i - 32767) % 32766 == 0)) continue;

                String[] arr = line.split(DELIM);
                if(i == 1) {
                    header = arr;
                } else if(i > 1) {
                    try {
                        result.put(Long.parseLong(arr[1].trim()), Long.parseLong(arr[2].trim()));
                    } catch (ArrayIndexOutOfBoundsException e) {
                        System.out.println(line);
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("End load data. " + i + " rows were processed");
        }
        return result;
    }

    private static void checkRTDCategoryAssoc(Set<Long> items, Multimap<Long, Long> longLongMultimap) {
        Map<String, MutableInteger> categoryCountMap = new HashMap<>();
        Map<String, Map<String, MutableInteger>> categoryCountAssocMap = new HashMap<>();

        Map<String, Set<Long>> mapSet = new HashMap<>();

        int emptyCount = 0;
        for (Long item : items) {
            Product product = productMap.get(item);
            if (product != null) {
                String category = product.getCategory();
                categoryCountMap.putIfAbsent(category, new MutableInteger(0));
                categoryCountMap.get(category).incrementAndGet();
                categoryCountAssocMap.putIfAbsent(category, new HashMap<>());

                for (Long value : longLongMultimap.get(item)) {
                    Product product1 = productMap.get(value);
                    if (product1 != null) {
                        mapSet.putIfAbsent(category, new HashSet<>());
                        if(mapSet.get(category).add(product1.getId())) {
                            categoryCountAssocMap.get(category).putIfAbsent(product1.getCategory(), new MutableInteger(0));
                            categoryCountAssocMap.get(category).get(product1.getCategory()).incrementAndGet();
                        }
                    }
                }
            } else {
                emptyCount++;
            }
        }
        System.out.println(emptyCount);

        List<Map.Entry<String, MutableInteger>> categoryCountList = new ArrayList<>(categoryCountMap.entrySet());
        Collections.sort(categoryCountList, (o1, o2) -> o2.getValue().getValue() - o1.getValue().getValue());

        for (Map.Entry<String, MutableInteger> entry : categoryCountList.subList(0, Math.min(categoryCountList.size(), 50))) {
            List<Map.Entry<String, MutableInteger>> categoryCountAssocList = new ArrayList<>(categoryCountAssocMap
                    .get(entry.getKey()).entrySet());
            Collections.sort(categoryCountAssocList, (o1, o2) -> o2.getValue().getValue() - o1.getValue().getValue());
            System.out.print("\"" + entry.getKey() + "\"=" + entry.getValue() + " => ");
            for (Map.Entry<String, MutableInteger> integerEntry : categoryCountAssocList.subList(0,
                    Math.min(categoryCountAssocList.size(), 20))) {
                System.out.print("\"" + integerEntry.getKey() + "\"=" + integerEntry.getValue() + ", ");
            }
            System.out.println(" ");
        }
    }

    private static Set<Long> generateSet(Long... integers) {
        Set<Long> result = new HashSet<>();
        Collections.addAll(result, integers);
        return result;
    }
}