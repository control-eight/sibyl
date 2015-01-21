package com.my.sibyl.itemsets.legacy;

import com.google.common.collect.Ordering;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author abykovsky
 * @since 11/24/14
 */
public class Main {

    private static final int LEVEL_THRESHOLD = 0;

    private static final int SLIDING_WINDOW_SIZE = 5_000_000;

    private static final ItemSetsFilter itemSetsFilter = new ItemSetsFilter();

    private static final FrequentItemSetsGenerator frequentItemSetsGenerator
            = new FrequentItemSetsGenerator(LEVEL_THRESHOLD, itemSetsFilter, SLIDING_WINDOW_SIZE, 2);

    //private static final double MIN_SUPPORT = 0.000006;
    private static final double MIN_SUPPORT = 2;
    private static final double MIN_CONFIDENCE = 0.01;

    private static final CandidatesGenerator candidatesGenerator
            = new CandidatesGenerator(MIN_SUPPORT, MIN_CONFIDENCE, frequentItemSetsGenerator, itemSetsFilter);

    private static final int K_TOP = 1;

    private static final int MAX_BASKET_ITEMS_COUNT = 7;

    private static int transactionCount;

    public static void main(String[] args) throws IOException {
        String line;
        Scanner scanner = new Scanner(System.in);
        //itemSetsFilter.loadMasterProducts("/Users/abykovsky/Downloads/RTD_PRODUCTS_A.dsv");
        //loadData("/Users/abykovsky/export_2011-08-14_2011-09-14.csv");
        //loadData("/Users/abykovsky/export_2011-11-29_2011-12-06.csv");
        //loadData("/Users/abykovsky/export_2013-11-29_2013-12-02.csv");
        loadData("/Users/abykovsky/export_2014-07-07_2014-07-15.csv");
        while (scanner.hasNext()) {
            line = scanner.nextLine();
            if(line.startsWith("r:")) {
                line = line.substring("r:".length());
                List<Pair<Long, Double>> globalTrends = Collections.emptyList();
                if(line.contains("|")) {
                    globalTrends = getGlobalTrends(line.substring(line.indexOf("|") + 1));
                }
                if(!globalTrends.isEmpty()) {
                    line = line.substring(0, line.indexOf("|"));
                }

                Set<Long> basketItems = getBasketItems(line);

                List<Container> topRecommendations = candidatesGenerator
                        .getTopRecommendations(basketItems, K_TOP, transactionCount);

                if(!topRecommendations.isEmpty()) {
                    subtract(topRecommendations, globalTrends);
                    while (topRecommendations.size() < K_TOP) {
                        topRecommendations.addAll(candidatesGenerator
                                .getTopRecommendations(basketItems, (K_TOP - topRecommendations.size())*2, transactionCount));
                        subtract(topRecommendations, globalTrends);
                        if(topRecommendations.isEmpty()) break;
                    }
                }
                List<Long> resultTopRecommendations = mergeLocalTrendsWithGlobal(topRecommendations, globalTrends);

                System.out.println("Recommendations: " + resultTopRecommendations);
            } else if(line.startsWith("s:")) {
                List<Long> transactionItems = getSuccessfulTransactionItems(line.substring("s:".length()));

                Map<Set<Long>, Long> map = new HashMap<>();
                map.put(new HashSet<>(transactionItems.subList(0, transactionItems.size() - 1)),
                        transactionItems.get(transactionItems.size() - 1));
                candidatesGenerator.process(map);
                candidatesGenerator.print();
            } else {
                int times = 1;
                Pattern pattern = Pattern.compile("^.*\\{(\\d+)\\}$");
                Matcher m = pattern.matcher(line);
                if(m.find()) {
                    times = Integer.parseInt(m.group(1));
                }
                List<Long> transactionItems = getSuccessfulTransactionItems(line);

                for(int i = 0; i < times; i++) {
                    Map<Set<Long>, Integer> difference = frequentItemSetsGenerator.process(transactionItems);
                    frequentItemSetsGenerator.print();
                    System.out.println("Difference: " + difference);
                    candidatesGenerator.process(difference, ++transactionCount);
                    candidatesGenerator.print();
                }
            }
        }
    }

    private static List<Long> mergeLocalTrendsWithGlobal(List<Container> topRecommendations, List<Pair<Long, Double>> globalTrends) {

        List<Container> list = new ArrayList<>(topRecommendations);
        for (Container topRecommendation : list) {
            topRecommendation.getGlobalScore().setConfidence(Math.min(topRecommendation.getGlobalScore().getConfidence() * 1.25, 1.0));
        }
        for (Pair<Long, Double> globalTrend : globalTrends) {
            list.add(new Container(globalTrend.getKey(), new GlobalScore(0, globalTrend.getValue())));
        }

        List<Container> kTop = Ordering.natural().greatestOf(list, K_TOP);
        List<Long> result = new ArrayList<>();
        for (Container container : kTop) {
            result.add(container.getItem());
        }

        return result;
    }

    private static void subtract(List<Container> topRecommendations, List<Pair<Long, Double>> globalTrends) {
        for(Iterator<Container> iter = topRecommendations.iterator(); iter.hasNext();) {
            Container container = iter.next();
            for (Pair<Long, Double> globalTrend : globalTrends) {
                if(globalTrend.getKey().equals(container.getItem())) {
                    iter.remove();
                    break;
                }
            }
        }
    }

    private static List<Long> getItems(String line) {
        List<Long> result = new ArrayList<>();
        String[] split = line.split("\\s");
        for(int i = 0; i < split.length; i++) {
            if("1".equals(split[i])) {
                result.add((long)i);
            }
        }
        return result;
    }

    private static Set<Long> getBasketItems(String line) {
        line = line.trim();
        Set<Long> result = new HashSet<>();
        String[] split = line.split("\\s");
        for (String s : split) {
            result.add(Long.parseLong(s.trim()));
        }
        return result;
    }

    private static List<Long> getSuccessfulTransactionItems(String line) {
        line = line.trim();
        List<Long> result = new ArrayList<>();
        String[] split = line.split("\\s");
        for (String s : split) {
            result.add(Long.parseLong(s.trim()));
        }
        return result;
    }

    private static List<Pair<Long, Double>> getGlobalTrends(String line) {
        line = line.trim();
        List<Pair<Long, Double>>  result = new ArrayList<>();
        String[] split = line.split("\\s");
        for (String s : split) {
            String[] arr = s.trim().split("=");
            result.add(new ImmutablePair<>(Long.parseLong(arr[0].trim()), Double.parseDouble(arr[1].trim())));
        }
        return result;
    }

    private static void loadData(String csvFile) throws IOException {
        try(Reader in = new FileReader(csvFile)) {
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader().parse(in);
            System.out.println("Start load data from \"" + csvFile + "\"");
            int i = 0;
            int all = 0;
            int j = 0;
            int orderWithFreeCount = 0;
            int freeCount = 0;
            int allProductCount = 0;
            Map<String, MutableInteger> deptMap = new HashMap<>();
            Set<Long> uniqueSet = new HashSet<>();
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

                if(transactionItems.contains(780327l) && transactionItems.contains(1170384l)) {
                    System.out.println(record.get("ORDER_ID"));
                    System.out.println(record.get("PROD_PRCHSD"));
                    System.out.println(record.get("PROD_REG_PRCS"));
                    System.out.println(record.get("PROD_ACTL_PRCS"));
                    System.out.println(record.get("PRICE_TYPE"));
                }

                uniqueSet.addAll(transactionItems);

                itemSetsFilter.filterFreePrice(transactionItems, actualPriceList);

                for (String s : record.get("DEPT_OF_PROD").split("\\|")) {
                    s = s.trim();
                    if (s.length() > 0) {
                        deptMap.putIfAbsent(s, new MutableInteger(0));
                        deptMap.get(s).incrementAndGet();
                    }
                }

                all++;
                if (transactionItems.size() > MAX_BASKET_ITEMS_COUNT) {
                    continue;
                }

                allProductCount += transactionItems.size();
                boolean isThereFree = false;
                for (Double price : actualPriceList) {
                    if (price == 0.0) {
                        isThereFree = true;
                        freeCount++;
                    }

                }
                if (isThereFree) orderWithFreeCount++;

                if(transactionItems.contains(614132l) && transactionItems.contains(803l)) {
                    //System.out.println(record.get("ORDER_ID"));
                }

                Map<Set<Long>, Integer> difference = frequentItemSetsGenerator.process(transactionItems);
                candidatesGenerator.process(difference, ++transactionCount);
                if (++i % 10000 == 0) System.out.println("10000 rows were processed");
                //if(i == 2000) break;
            }

            //System.out.println(frequentItemSetsGenerator.getCount(generateSet(561452, 612302, 612373)));
            //System.out.println(frequentItemSetsGenerator.getCount(generateSet(561452, 612302, 612373, 580147)));

            candidatesGenerator.getTopRecommendations(generateSet(562545, 577170, 564246), K_TOP, transactionCount);
            candidatesGenerator.update(K_TOP, transactionCount);

            candidatesGenerator.print();
            frequentItemSetsGenerator.printShort(all);
            //System.out.println("603401: " + j);
            //System.out.println(frequentItemSetsGenerator.getCount(generateSet(603401)));
            //System.out.println(frequentItemSetsGenerator.getCount(generateSet(603401, 639816)));
            //System.out.println(frequentItemSetsGenerator.getCount(generateSet(603401, 614111)));
            //System.out.println(frequentItemSetsGenerator.getCount(generateSet(603401, 614110)));
            //System.out.println(frequentItemSetsGenerator.getCount(generateSet(603401, 634963)));
            System.out.println("End load data. " + i + " rows were processed from " + all);
            System.out.println("Free count: " + freeCount + " from " + allProductCount);
            System.out.println("Order with free count: " + orderWithFreeCount + " from " + i);
            System.out.println("Unique products count: " + uniqueSet.size());

            System.out.println(frequentItemSetsGenerator.getCount(generateSet(525053)));

            ArrayList<Map.Entry<String, MutableInteger>> list = new ArrayList<>(deptMap.entrySet());
            Collections.sort(list, (o1, o2) -> o2.getValue().getValue() - o1.getValue().getValue());
            System.out.println(list);
        }
    }

    private static Set<Long> generateSet(Integer ... integers) {
        Set<Long> result = new HashSet<>();
        for (Integer integer : integers) {
            result.add((long) integer);
        }
        return result;
    }
}
