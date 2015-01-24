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

    public Collection<ItemSetAndAssociation<T>> generateItemSetsAndAssociations(List<T> transactionItems, long count) {
        List<Set<T>> itemSets = new ArrayList<>();
        List<T> lastItemInItemSets = new ArrayList<>();
        for (T item : transactionItems) {
            Set<T> set = new HashSet<>();
            set.add(item);
            itemSets.add(set);
            lastItemInItemSets.add(item);
        }

        Map<Set<T>, Long> generated = new HashMap<>();
        generateItemSets(itemSets, lastItemInItemSets, transactionItems, generated, count);
        Collection<ItemSetAndAssociation<T>> result = generateAssociations(generated, transactionItems, count);
        return result;
    }

    public Collection<Set<T>> generateItemSets(List<T> basketItems) {
        List<Set<T>> itemSets = new ArrayList<>();
        List<T> lastItemInItemSets = new ArrayList<>();
        for (T item : basketItems) {
            Set<T> set = new HashSet<>();
            set.add(item);
            itemSets.add(set);
            lastItemInItemSets.add(item);
        }

        Map<Set<T>, Long> generated = new HashMap<>();
        generateItemSets(itemSets, lastItemInItemSets, basketItems, generated, 0);
        return generated.keySet();
    }

    private Collection<ItemSetAndAssociation<T>> generateAssociations(Map<Set<T>, Long> generated, List<T> transactionItems, long count) {
        Collection<ItemSetAndAssociation<T>> result = new HashSet<>();
        for (Map.Entry<Set<T>, Long> entry : generated.entrySet()) {

            ItemSetAndAssociation<T> itemSetAndAssociation = new ItemSetAndAssociation<>();
            itemSetAndAssociation.setItemSet(entry.getKey());
            itemSetAndAssociation.setCount(entry.getValue());

            Map<T, Long> associationMap = new HashMap<>();
            for (T transactionItem : transactionItems) {
                if(!entry.getKey().contains(transactionItem)) {
                    associationMap.put(transactionItem, count);
                }
            }
            itemSetAndAssociation.setAssociationMap(associationMap);
            result.add(itemSetAndAssociation);
        }
        return result;
    }

    private void generateItemSets(List<Set<T>> itemSets, List<T> lastItemInItemSets,
                                  List<T> transactionItems, Map<Set<T>, Long> generated, long count) {
        for (int i = 0; i < itemSets.size(); i++) {
            Set<T> itemSet = itemSets.get(i);

            if(itemSet.size() > maxItemSetLength) continue;

            generated.put(itemSet, count);

            List<T> newLastItemInItemSets = new ArrayList<>();
            List<Set<T>> nextLevelItemSets = generateNextLevel(itemSet, transactionItems,
                    lastItemInItemSets.get(i), newLastItemInItemSets);
            generateItemSets(nextLevelItemSets, newLastItemInItemSets, transactionItems, generated, count);
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
