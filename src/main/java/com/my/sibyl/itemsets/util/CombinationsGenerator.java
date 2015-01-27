package com.my.sibyl.itemsets.util;

import com.my.sibyl.itemsets.ItemSetAndAssociation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public class CombinationsGenerator<T> {

    private int maxItemSetLength;

    public CombinationsGenerator(int maxCombinationLength) {
        this.maxItemSetLength = maxCombinationLength;
    }

    public Collection<Set<T>> generateCombinations(List<T> itemList) {
        List<Set<T>> itemSets = new ArrayList<>();
        List<T> lastItemInItemSets = new ArrayList<>();
        for (T item : itemList) {
            Set<T> set = new HashSet<>();
            set.add(item);
            itemSets.add(set);
            lastItemInItemSets.add(item);
        }

        List<Set<T>> generated = new ArrayList<>();
        generateItemSets(itemSets, lastItemInItemSets, itemList, generated);
        return generated;
    }

    private void generateItemSets(List<Set<T>> itemSets, List<T> lastItemInItemSets,
                                  List<T> transactionItems, List<Set<T>> generated) {
        for (int i = 0; i < itemSets.size(); i++) {
            Set<T> itemSet = itemSets.get(i);

            if(itemSet.size() > maxItemSetLength) continue;

            generated.add(itemSet);

            List<T> newLastItemInItemSets = new ArrayList<>();
            List<Set<T>> nextLevelItemSets = generateNextLevel(itemSet, transactionItems,
                    lastItemInItemSets.get(i), newLastItemInItemSets);
            generateItemSets(nextLevelItemSets, newLastItemInItemSets, transactionItems, generated);
        }
    }

    //generate combinations for particular level
    private List<Set<T>> generateNextLevel(Set<T> curLevelItemSet, List<T> transactionItems,
                                                 T lastItemInItemSet, List<T> newLastItemInItemSets) {
        List<Set<T>> result = new ArrayList<>();

        List<T> nextLevelItemList = new ArrayList<>(curLevelItemSet);

        boolean found = false;
        for (T newItem : transactionItems) {
            if(!found && !lastItemInItemSet.equals(newItem)) {
                continue;
            }
            if(lastItemInItemSet.equals(newItem)) {
                found = true;
                continue;
            }

            Set<T> nextLevelItemSet = new HashSet<>(nextLevelItemList);
            nextLevelItemSet.add(newItem);
            newLastItemInItemSets.add(newItem);
            result.add(nextLevelItemSet);
        }

        return result;
    }
}
