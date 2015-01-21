package com.my.sibyl.itemsets.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author abykovsky
 * @since 1/22/15
 */
public class PermutationsGenerator<T> {

    private int maxCombinationLength;

    public PermutationsGenerator(int maxCombinationLength) {
        this.maxCombinationLength = maxCombinationLength;
    }

    public Map<List<T>, Integer> generateItemSetsAndAssociations(List<T> transactionItems, int addAmount) {
        List<List<T>> itemSets = new ArrayList<>();
        for (T item : transactionItems) {
            List<T> set = new ArrayList<>();
            set.add(item);
            itemSets.add(set);
        }

        Map<List<T>, Integer> generated = new HashMap<>();
        generateItemSets(itemSets, transactionItems, generated, addAmount);
        return generated;
    }

    private void generateItemSets(List<List<T>> itemSets, List<T> transactionItems,
                                  Map<List<T>, Integer> generated, int addAmount) {
        for (int i = 0; i < itemSets.size(); i++) {
            List<T> itemSet = itemSets.get(i);

            generated.put(itemSet, addAmount);

            if(itemSet.size() >= maxCombinationLength) continue;

            List<List<T>> nextLevelItemSets = generateNextLevel(itemSet, transactionItems);
            generateItemSets(nextLevelItemSets, transactionItems, generated, addAmount);
        }
    }

    //generate combinations for particular level
    private List<List<T>> generateNextLevel(List<T> curLevelItemSet, List<T> transactionItems) {
        List<List<T>> result = new ArrayList<>();

        List<T> nextLevelItemList = new ArrayList<>(curLevelItemSet);

        for (T newItem : transactionItems) {
            List<T> nextLevelItemSet = new ArrayList<>(nextLevelItemList);
            if(!nextLevelItemList.contains(newItem)) {
                nextLevelItemSet.add(newItem);
                result.add(nextLevelItemSet);
            }
        }

        return result;
    }
}
