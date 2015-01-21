package com.my.sibyl.itemsets.analytics;

import com.my.sibyl.itemsets.legacy.CandidatesGenerator;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author abykovsky
 * @since 12/4/14
 */
public class NewProductsCalculation {

    private static final String DELIM = "~\\|~";

    private static Map<Long, Product> productMap = null;

    private static final int LEVEL_THRESHOLD = 0;

    private static final int SLIDING_WINDOW_SIZE = 2_000_000;

    private static final int MAX_BASKET_ITEMS_COUNT = 6;

    private static final ItemSetsFilter itemSetsFilter = new ItemSetsFilter();

    //private static final double MIN_SUPPORT = 0.000006;
    private static final double MIN_SUPPORT = 2;
    private static final double MIN_CONFIDENCE = 0.01;

    private static final int K_TOP = 1;


    private static final FrequentItemSetsGenerator frequentItemSetsGenerator
            = new FrequentItemSetsGenerator(LEVEL_THRESHOLD, itemSetsFilter, SLIDING_WINDOW_SIZE, 2);

    private static final CandidatesGenerator candidatesGenerator
            = new CandidatesGenerator(MIN_SUPPORT, MIN_CONFIDENCE, frequentItemSetsGenerator, itemSetsFilter);

    private static int transactionCount;

    public static void main(String[] args) throws IOException, ParseException {
        productMap = loadProducts("/Users/abykovsky/Downloads/RTD_PRODUCTS_A.dsv");
        itemSetsFilter.setMasterProducts(generate(productMap));
        //System.out.println( productMap.get(1219169l).getCategory());
        loadData("/Users/abykovsky/export_2013-11-29_2013-12-02.csv", "30-NOV-13");
        //loadData("/Users/abykovsky/export_2014-07-07_2014-07-15.csv");
        //loadData("/Users/abykovsky/export_2014-04-11_2014-04-18.csv");
    }

    private static Map<Long, Long> generate(Map<Long, Product> productMap) {
        Map<Long, Long> result = new HashMap<>();
        for (Map.Entry<Long, Product> entry : productMap.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getMasterProductId());
        }
        return result;
    }

    private static void loadData(String csvFile, String firstDate) throws IOException, ParseException {

        Set<Long> newProducts = filterNewProducts(firstDate);
        Map<Long, MutableInteger> transactionItemMap = new HashMap<>();
        int newProductsBuyCount = 0;
        int newProductsSingleBuyCount = 0;
        int newProductsFreeBuyCount = 0;
        int newProductsFreeSingleBuyCount = 0;
        long itemsCount = 0;
        Set<Long> newProductsUniqBuyCountSet = new HashSet<>();
        try (Reader in = new FileReader(csvFile)) {
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader().parse(in);
            System.out.println("Start load data from \"" + csvFile + "\"");
            int all = 0;

            Map<Integer, AtomicInteger> countMap = new TreeMap<>();
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
                        long key = Long.parseLong(s);
                        transactionItems.add(key);
                    }
                }
                /*itemsCount += transactionItems.size();
                countMap.putIfAbsent(transactionItems.size(), new AtomicInteger(0));
                countMap.get(transactionItems.size()).incrementAndGet();*/

                int iter = 0;
                for (String s : record.get("QUANTITIES").split("\\|")) {
                    s = s.trim();
                    if (s.length() > 0) {
                        Integer value = Integer.parseInt(s);
                        Long key = transactionItems.get(iter);
                        if(!isFreePrice(iter, actualPriceList) && newProducts.contains(key)) {
                            newProductsSingleBuyCount++;
                            newProductsBuyCount += value;
                            transactionItemMap.putIfAbsent(key, new MutableInteger(0));
                            transactionItemMap.get(key).incrementAndGet();
                            newProductsUniqBuyCountSet.add(key);
                        }
                        if(isFreePrice(iter, actualPriceList) && newProducts.contains(key)) {
                            newProductsFreeSingleBuyCount++;
                            newProductsFreeBuyCount += value;
                        }
                    }
                    iter++;
                }

                filterFreePrice(transactionItems, actualPriceList);

                itemsCount += transactionItems.size();
                countMap.putIfAbsent(transactionItems.size(), new AtomicInteger(0));
                countMap.get(transactionItems.size()).incrementAndGet();

                all++;
                if (transactionItems.size() > MAX_BASKET_ITEMS_COUNT) {
                    continue;
                }

                Map<Set<Long>, Integer> difference = frequentItemSetsGenerator.process(transactionItems);
                candidatesGenerator.process(difference, ++transactionCount);
            }
            System.out.println((double)itemsCount/all);
            System.out.println(countMap);
            System.out.println("End load data. " + all + " rows were processed.");

            /*System.out.println("End load data. " + all + " rows were processed.");
            candidatesGenerator.update(K_TOP, transactionCount);
            candidatesGenerator.print();

            Set<Long> newProductsWithRecommendations = new HashSet<>();
            int recommSingleBuyCount = 0;
            Set<Long> recommUniqBuyCountSet = new HashSet<>();
            for (Map.Entry<Long, MutableInteger> entry : transactionItemMap.entrySet()) {
                if (!candidatesGenerator.getTopRecommendations(generateSet(entry.getKey()), K_TOP,
                        transactionCount).isEmpty()) {
                    newProductsWithRecommendations.add(entry.getKey());
                    recommSingleBuyCount += entry.getValue().getValue();
                    recommUniqBuyCountSet.add(entry.getKey());
                }
            }
            System.out.println("newProductsSingleBuyCount: " + newProductsSingleBuyCount);
            System.out.println("newProductsBuyCount: " + newProductsBuyCount);
            System.out.println("newProductsFreeSingleBuyCount: " + newProductsFreeSingleBuyCount);
            System.out.println("newProductsFreeBuyCount: " + newProductsFreeBuyCount);
            System.out.println("newProductsUniqBuyCountSet: " + newProductsUniqBuyCountSet.size());
            System.out.println("recommSingleBuyCount: " + recommSingleBuyCount);
            System.out.println("recommUniqBuyCountSet: " + recommUniqBuyCountSet.size());
            System.out.println("New product with recommendations: " + newProductsWithRecommendations.size() + " from "
                    + newProducts.size());*/
        }
    }

    private static Set<Long> filterNewProducts(String firstDateStr) throws ParseException {
        Date endDate = new SimpleDateFormat("dd-MMM-yy").parse(firstDateStr);
        Calendar c = Calendar.getInstance();
        c.setTime(endDate);
        c.add(Calendar.DAY_OF_YEAR, -10);
        Date startDate = c.getTime();

        Set<Long> result = new HashSet<>();
        for (Map.Entry<Long, Product> entry : productMap.entrySet()) {
            if(entry.getValue().getFirstLiveDate().after(startDate)
                    && entry.getValue().getFirstLiveDate().before(endDate)) {
                result.add(entry.getKey());
            }
        }
        System.out.println("New products size: " + result.size());
        return result;
    }

    private static void filterFreePrice(List<Long> transactionItems, List<Double> actualPriceList) {
        for(int i = 0; i < transactionItems.size(); i++) {
            if(actualPriceList.get(i) == 0.0) {
                transactionItems.remove(i);
                actualPriceList.remove(i);
                i--;
            }
        }
    }

    private static boolean isFreePrice(int transactionItem, List<Double> actualPriceList) {
        return actualPriceList.get(transactionItem) == 0.0;
    }

    public static Map<Long, Product> loadProducts(String csvFile) throws IOException, ParseException {
        System.out.println("Start load data from \"" + csvFile + "\"");
        Map<Long, Product> result = new HashMap<>();
        SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yy");
        try (Reader in = new FileReader(csvFile)) {
            BufferedReader bf = new BufferedReader(in);
            String line;
            int i = -1;
            String[] header = null;
            while ((line = bf.readLine()) != null) {
                i++;
                if(line.trim().length() == 0) continue;
                if(i == 32767 || (i > 32767 && (i - 32767) % 32766 == 0)) continue;

                //71-72
                String[] arr = line.split(DELIM);
                if(i == 1) {
                    header = arr;
                } else if(i > 1) {
                    String str = arr[3].trim();
                    Long masterProductId = null;
                    try {
                        if(!str.isEmpty()) {
                            masterProductId = Long.parseLong(str);
                        }
                    } catch (NumberFormatException ex) {
                        ex.printStackTrace();
                    }
                    Long productId = Long.parseLong(arr[2].trim());

                    Product product = new Product();
                    product.setId(productId);
                    product.setMasterProductId(masterProductId);
                    product.setCategory(arr[14]);
                    String source = arr[64];
                    if(!source.trim().isEmpty()) {
                        product.setFirstLiveDate(format.parse(source));
                    } else {
                        product.setFirstLiveDate(new Date());
                    }

                    result.putIfAbsent(productId, product);
                }
            }
            System.out.println("End load data. " + i + " rows were processed");
        }
        return result;
    }

    private static Set<Long> generateSet(Long ... integers) {
        Set<Long> result = new HashSet<>();
        Collections.addAll(result, integers);
        return result;
    }
}
