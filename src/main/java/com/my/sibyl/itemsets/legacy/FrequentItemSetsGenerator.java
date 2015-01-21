package com.my.sibyl.itemsets.legacy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
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

    private final Deque<long[]> pastTransactionList;

    private final Map<Set<Long>, MutableInteger> storedItemSets = new HashMap<>();

    private final Integer levelThreshold;

    private final ItemSetsFilter itemSetsFilter;

    private static int sizeOneCount;

    private final Integer slidingWindowSize;

    private final int maxSetSize;

    public FrequentItemSetsGenerator(final Integer levelThreshold, final ItemSetsFilter itemSetsFilter,
                                     final Integer slidingWindowSize, final int maxSetSize) {
        this.levelThreshold = levelThreshold;
        this.itemSetsFilter = itemSetsFilter;
        this.slidingWindowSize = slidingWindowSize;
        this.pastTransactionList = new ArrayDeque<>(slidingWindowSize);
        this.maxSetSize = maxSetSize;
    }

    public Map<Set<Long>, Integer> process(List<Long> transactionItems) {
        long[] pastTransactionToRemove = addPastTransaction(transactionItems);

        subtractFromItemSets(pastTransactionToRemove);
        Map<Set<Long>, Integer> difference = addToItemSets(transactionItems, 1);

        return difference;
    }

    private Map<Set<Long>, Integer> subtractFromItemSets(long[] pastTransactionToRemove) {
        Map<Set<Long>, Integer> subtractDifference = null;
        if (pastTransactionToRemove != null) {
            List<Long> longArr = new ArrayList<>();
            for (long l : pastTransactionToRemove) {
                longArr.add(l);
            }
            subtractDifference = addToItemSets(longArr, -1);
        }
        return subtractDifference;
    }

    private Map<Set<Long>, Integer> addToItemSets(List<Long> transactionItems, int addAmount) {
        List<Set<Long>> itemSets = new ArrayList<>();
        List<Long> lastItemInItemSets = new ArrayList<>();
        for (Long item : transactionItems) {
            Set<Long> set = new HashSet<>();
            set.add(item);
            itemSets.add(set);
            lastItemInItemSets.add(item);
        }

        Map<Set<Long>, Integer> generated = new HashMap<>();
        generateItemSets(itemSets, lastItemInItemSets, transactionItems, generated, addAmount);

        Map<Set<Long>, Integer> difference = new HashMap<>();
        updateItemSets(generated, difference);
        return difference;
    }

    private long[] addPastTransaction(List<Long> transactionItems) {
        long[] pastTransaction = new long[transactionItems.size()];
        for (int i = 0; i < pastTransaction.length; i++) {
            pastTransaction[i] = transactionItems.get(i);
        }

        long[] pastTransactionToRemove = null;
        if(pastTransactionList.size() == slidingWindowSize) {
            pastTransactionToRemove = pastTransactionList.removeFirst();
        }
        pastTransactionList.addLast(pastTransaction);

        return pastTransactionToRemove;
    }

    private void updateItemSets(Map<Set<Long>, Integer> generated, Map<Set<Long>, Integer> difference) {

        for (Map.Entry<Set<Long>, Integer> entry : generated.entrySet()) {
            Set<Long> itemSet = entry.getKey();

            if(itemSetsFilter.filterMasterProduct(itemSet)) continue;

            final int freq = updateStoredItemSets(itemSet, entry.getValue());

            //itemsets size = 1 are used only to open new level when exceed level threshold
            if(itemSet.size() > 1) {
                difference.put(itemSet, freq);
            }

            //subtracting mode
            if(freq == 0) {
                storedItemSets.remove(itemSet);
            }
        }
    }

    private void generateItemSets(List<Set<Long>> itemSets, List<Long> lastItemInItemSets,
                                  List<Long> transactionItems, Map<Set<Long>, Integer> generated, int addAmount) {
        for (int i = 0; i < itemSets.size(); i++) {
            Set<Long> itemSet = itemSets.get(i);

            generated.put(itemSet, addAmount);

            if(itemSet.size() >= maxSetSize) continue;

            List<Long> newLastItemInItemSets = new ArrayList<>();
            List<Set<Long>> nextLevelItemSets = generateNextLevel(itemSet, transactionItems,
                    lastItemInItemSets.get(i), newLastItemInItemSets);
            generateItemSets(nextLevelItemSets, newLastItemInItemSets, transactionItems, generated, addAmount);
        }
    }

    private int updateStoredItemSets(Set<Long> itemSet, int addAmount) {
        if(!storedItemSets.containsKey(itemSet) && itemSet.size() == 1) {
            sizeOneCount++;
        }
        storedItemSets.putIfAbsent(itemSet, new MutableInteger(0));
        int amount = storedItemSets.get(itemSet).addAndGet(addAmount);
        if(amount < 0) {
            System.err.println("Amount can't be less than zero");
            storedItemSets.get(itemSet).setValue(0);
        }
        return amount;
    }

    //generate combinations for particular level
    private List<Set<Long>> generateNextLevel(Set<Long> curLevelItemSet, List<Long> transactionItems,
                                              Long lastItemInItemSet, List<Long> newLastItemInItemSets) {
        List<Set<Long>> result = new ArrayList<>();

        List<Long> nextLevelItemList = new ArrayList<>(curLevelItemSet);

        //if (freq >= levelThreshold) {
            boolean found = false;
            for (Long newItem : transactionItems) {
                if(!found && !lastItemInItemSet.equals(newItem)) {
                    continue;
                }
                if(lastItemInItemSet.equals(newItem)) {
                    found = true;
                    continue;
                }

                Set<Long> nextLevelItemSet = new HashSet<>(nextLevelItemList);
                nextLevelItemSet.add(newItem);
                newLastItemInItemSets.add(newItem);
                result.add(nextLevelItemSet);
            }
        //}

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
