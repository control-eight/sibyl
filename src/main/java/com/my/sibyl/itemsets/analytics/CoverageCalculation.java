package com.my.sibyl.itemsets.analytics;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.my.sibyl.itemsets.CandidatesGenerator;
import com.my.sibyl.itemsets.FrequentItemSetsGenerator;
import com.my.sibyl.itemsets.ItemSetsFilter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 12/3/14
 */
public class CoverageCalculation {

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
        //loadData("/Users/abykovsky/export_2013-11-29_2013-12-02.csv");
        //loadData("/Users/abykovsky/export_2014-07-07_2014-07-15.csv");
        loadData("/Users/abykovsky/export_2014-04-11_2014-04-18.csv");
    }

    private static Map<Long, Long> generate(Map<Long, Product> productMap) {
        Map<Long, Long> result = new HashMap<>();
        for (Map.Entry<Long, Product> entry : productMap.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getMasterProductId());
        }
        return result;
    }

    private static void loadData(String csvFile) throws IOException, ParseException {

        loadFirstPartOfData(csvFile);

        Map<Long, OrderProduct> map = new HashMap<>();
        int continuesCoverage = 0;
        int productCount = 0;
        try (Reader in = new FileReader(csvFile)) {
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader().parse(in);
            System.out.println("Start load data from \"" + csvFile + "\"");
            int all = 0;
            int allQty = 0;

            SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yy");
            Date startDate = format.parse("14-APR-14");
            Date endDate = format.parse("18-APR-14");
            for (CSVRecord record : records) {

                try {
                    Date date = format.parse(record.get("ORDER_DTTM"));
                    if(date.before(startDate) || date.after(endDate)) {
                        continue;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }

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
                    productCount++;
                }

                int iter = 0;
                for (String s : record.get("QUANTITIES").split("\\|")) {
                    s = s.trim();
                    if (s.length() > 0) {
                        if(!isFreePrice(iter, actualPriceList)) {
                            int value = Integer.parseInt(s);
                            allQty += value;
                            Long key = transactionItems.get(iter);
                            map.putIfAbsent(key, new OrderProduct(key));
                            map.get(key).getQuantity().addAndGet(value);

                            if (!candidatesGenerator.getTopRecommendations(generateSet(key), K_TOP, transactionCount).isEmpty()) {
                                //continuesCoverage += value;
                                continuesCoverage++;
                            }
                        }
                    }
                    iter++;
                }

                filterFreePrice(transactionItems, actualPriceList);

                all++;
                if (transactionItems.size() > MAX_BASKET_ITEMS_COUNT) {
                    continue;
                }

                Map<Set<Long>, Integer> difference = frequentItemSetsGenerator.process(transactionItems);
                candidatesGenerator.process(difference, ++transactionCount);
            }
            //candidatesGenerator.update(K_TOP, transactionCount);

            System.out.println("All qty: " + allQty);
            System.out.println("End load data. " + all + " rows were processed.");
            candidatesGenerator.print();
        }

        List<OrderProduct> arr = new ArrayList<>(map.values());
        Collections.sort(arr);

        //Multimap<Long, Long> rtdRecommendations = loadRTDRecommendations("/Users/abykovsky/Downloads/RTD_EXTNL_SYS_PROD_REC_A_PDP.dsv");

        int percQty = 0;
        int uniqueProductCount = 0;
        int coverage = 0;
        int countCoverage = 0;
        int rtdCoverage = 0;
        for (OrderProduct orderProduct : arr) {
            percQty += orderProduct.getQuantity().getValue();
            uniqueProductCount++;
            //if((double)percQty / allQty > 0.8) break;

            if(!candidatesGenerator.getTopRecommendations(generateSet(orderProduct.getId()), K_TOP, transactionCount).isEmpty()) {
                coverage++;
                countCoverage += orderProduct.getQuantity().getValue();
            }
                /*if(!rtdRecommendations.get(orderProduct.getId()).isEmpty()) {
                    rtdCoverage++;
                }*/
        }
        System.out.println("80 percentile (" + percQty + ") = " + uniqueProductCount);
        System.out.println("Coverage: " + ((double)coverage/productCount) + " count coverage: " + ((double)countCoverage/percQty));
        System.out.println("Continues coverage: " + (double) continuesCoverage/productCount);
        //System.out.println("RTD Coverage: " + ((double)rtdCoverage/productCount));
    }

    private static void loadFirstPartOfData(String csvFile) throws IOException, ParseException {
        try (Reader in = new FileReader(csvFile)) {
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader().parse(in);
            System.out.println("Start load data from \"" + csvFile + "\"" + " dates: 11-APR-14 - 15-APR-14");
            int all = 0;
            SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yy");
            Date startDate = format.parse("11-APR-14");
            Date endDate = format.parse("15-APR-14");

            for (CSVRecord record : records) {
                try {
                    Date date = format.parse(record.get("ORDER_DTTM"));
                    if(date.before(startDate) || date.after(endDate)) {
                        continue;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }

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

                Map<Set<Long>, Integer> difference = frequentItemSetsGenerator.process(transactionItems);
                candidatesGenerator.process(difference, ++transactionCount);
            }
            System.out.println("End load data. " + all + " rows were processed.");
        }
    }

    private static void filterFreePrice(List<Long> transactionItems, List<Double> actualPriceList) {
        for(int i = 0; i < transactionItems.size(); i++) {
            if(actualPriceList.get(i) == 0.0) {
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

    private static Set<Long> generateSet(Long ... integers) {
        Set<Long> result = new HashSet<>();
        Collections.addAll(result, integers);
        return result;
    }
}
