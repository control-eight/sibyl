package com.my.sibyl.itemsets.old;

import com.my.sibyl.itemsets.legacy.ItemSetsFilter;
import com.my.sibyl.itemsets.legacy.MutableInteger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TODO: thread safe. It isn't required if distributed version will be created
 * @author abykovsky
 * @since 11/24/14
 */
public class FrequentItemSetsGenerator {

    private final Map<Set<Long>, MutableInteger> storedItemSets = new HashMap<>();

    private final Integer levelThreshold;

    private final ItemSetsFilter itemSetsFilter;

    private static int sizeOneCount;

    public FrequentItemSetsGenerator(final Integer levelThreshold, final ItemSetsFilter itemSetsFilter) {
        this.levelThreshold = levelThreshold;
        this.itemSetsFilter = itemSetsFilter;
    }

    public Map<Set<Long>, Integer> process(List<Long> transactionItems) {
        List<Set<Long>> itemSets = new ArrayList<>();
        Set<Long> transactionItemSet = new HashSet<>(transactionItems);
        List<Long> lastItemInItemSets = new ArrayList<>();
        for (Long item : transactionItems) {
            Set<Long> set = new HashSet<>();
            set.add(item);
            itemSets.add(set);
            lastItemInItemSets.add(item);
        }

        Map<Set<Long>, Integer> difference = new HashMap<>();

        /*Set<Long> testSet = new HashSet<>();
        testSet.add(508870l);testSet.add(603278l);testSet.add(575603l);

        if(testSet.equals(transactionItemSet)) {
            System.out.println("");
        }*/

        updateItemSets(itemSets, lastItemInItemSets, transactionItems, difference);
        return difference;
    }

    private static int jj = 0;

    private void updateItemSets(List<Set<Long>> itemSets, List<Long> lastItemInItemSets,
                                List<Long> transactionItems, Map<Set<Long>, Integer> difference) {
        for (int i = 0; i < itemSets.size(); i++) {
            Set<Long> itemSet = itemSets.get(i);

            if(itemSetsFilter.filterMasterProduct(itemSet)) continue;


            /*Set<Long> testItemSet = new HashSet<>();
            testItemSet.add(603401l);
            if(itemSet.equals(testItemSet)) {
                System.out.println(itemSet + " " + ++jj + " " + storedItemSets.get(itemSet));
            }*/

            final int freq = updateStoredItemSets(itemSet);

            //itemsets size = 1 are used only to open new level when exceed level threshold
            if(itemSet.size() > 1) {
                difference.put(itemSet, freq);
            }

            List<Long> newLastItemInItemSets = new ArrayList<>();
            List<Set<Long>> nextLevelItemSets = generateNextLevel(itemSet, freq, transactionItems,
                    lastItemInItemSets.get(i), newLastItemInItemSets);
            updateItemSets(nextLevelItemSets, newLastItemInItemSets, transactionItems, difference);
        }
    }

    private int updateStoredItemSets(Set<Long> itemSet) {
        if(!storedItemSets.containsKey(itemSet) && itemSet.size() == 1) {
            sizeOneCount++;
        }
        storedItemSets.putIfAbsent(itemSet, new MutableInteger(0));
        return storedItemSets.get(itemSet).incrementAndGet();
    }

    //generate combinations for particular level
    private List<Set<Long>> generateNextLevel(Set<Long> curLevelItemSet, int freq, List<Long> transactionItems,
                                              Long lastItemInItemSet, List<Long> newLastItemInItemSets) {
        List<Set<Long>> result = new ArrayList<>();

        List<Long> nextLevelItemList = new ArrayList<>(curLevelItemSet);

        if (freq >= levelThreshold) {
            boolean found = false;
            for (Long newItem : transactionItems) {
                if(!found && !lastItemInItemSet.equals(newItem)) {
                    continue;
                }
                if(lastItemInItemSet.equals(newItem)) {
                    found = true;
                    continue;
                }

                //if (nextLevelItemList.contains(newItem)) continue;
                Set<Long> nextLevelItemSet = new HashSet<>(nextLevelItemList);
                nextLevelItemSet.add(newItem);
                newLastItemInItemSets.add(newItem);
                result.add(nextLevelItemSet);
            }
        }

        return result;
    }

    public Integer getCount(Set<Long> itemSet) {
        MutableInteger integer = storedItemSets.get(itemSet);
        return integer == null? 0: integer.getValue();
    }

    public void print() {
        System.out.println("ItemSets: " + storedItemSets);
    }

    public void printShort(int transactionCount) {
        System.out.println("StoredItemSets size: " + storedItemSets.size() + ". Size{1}: " + sizeOneCount);
        int freqOne = 0;
        int freqTwo = 0;
        int freqThree = 0;
        int freqFour = 0;
        int freqFive = 0;
        for (Map.Entry<Set<Long>, MutableInteger> entry : storedItemSets.entrySet()) {
            if(entry.getValue().getValue() == 1) {
                freqOne += entry.getValue().getValue();
            } else if(entry.getValue().getValue() == 2) {
                freqTwo += entry.getValue().getValue();
            } else if(entry.getValue().getValue() == 3) {
                freqThree += entry.getValue().getValue();
            } else if(entry.getValue().getValue() == 4) {
                freqFour += entry.getValue().getValue();
            } else if(entry.getValue().getValue() == 5) {
                freqFive += entry.getValue().getValue();
            }
        }
        System.out.println("FreqOne: " + freqOne + " FreqTwo: " + freqTwo + " FreqThree: " + freqThree
                + " FreqFour: " + freqFour + " FreqFive: " + freqFive);
        int all = freqOne + freqTwo + freqThree + freqFour + freqFive;
        System.out.println("FreqAll: " + all + " = " + ((double)all/transactionCount) + " {1} = " + ((double) freqOne)/transactionCount);
    }
}
