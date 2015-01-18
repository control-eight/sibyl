package com.my.sibyl.itemsets;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 12/1/14
 */
public class ItemSetsFilter {

    private static final String DELIM = "~\\|~";

    private Map<Long, Long> masterProducts = new HashMap<>();

    public Map<Long, Long> loadMasterProducts(String csvFile) throws IOException {
        System.out.println("Start load data from \"" + csvFile + "\"");
        Reader in = new FileReader(csvFile);
        try (BufferedReader bf = new BufferedReader(in)) {
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
                    /*Map<String, String> record = new HashMap<>();
                    for(int ii = 0; ii < arr.length; ii++) {
                        record.put(header[ii].trim(), arr[ii].trim());
                    }
                    String str = record.get("MASTERPRODUCTID");
                    Long masterProductId = null;
                    try {
                        if(!str.isEmpty()) {
                            masterProductId = Long.parseLong(str);
                        }
                    } catch (NumberFormatException ex) {
                        ex.printStackTrace();
                    }
                    Long productId = Long.parseLong(record.get("PRODUCTID"));
                    masterProducts.putIfAbsent(productId, masterProductId);*/

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
                    masterProducts.putIfAbsent(productId, masterProductId);
                }
            }
            System.out.println("End load data. " + i + " rows were processed");
        }
        return masterProducts;
    }

    public void filterFreePrice(List<Long> transactionItems, List<Double> actualPriceList) {
        for(int i = 0; i < transactionItems.size(); i++) {
            if(actualPriceList.get(i) == 0.0) {
                transactionItems.remove(i);
                i--;
            }
        }
    }

    /**
     * If any two products from this particular item sets share master product filter it.
     * @param itemSet
     * @return
     */
    public boolean filterMasterProduct(Set<Long> itemSet) {
        if(itemSet.size() == 1 || itemSet.isEmpty()) return false;

        Set<Long> masterProductSet = new HashSet<>();
        for (Long productId : itemSet) {
            Long masterProductId = masterProducts.get(productId);
            if(masterProductId == null) return false;
            if(!masterProductSet.add(masterProductId)) return true;
        }
        return false;
    }

    public void setMasterProducts(Map<Long, Long> masterProducts) {
        this.masterProducts = masterProducts;
    }
}
