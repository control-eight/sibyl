package com.my.sibyl.itemsets.util;

import com.google.common.collect.Lists;
import com.my.sibyl.itemsets.ItemSetAndAssociation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 1/27/15
 */
public class ItemSetsGenerator {

    private CombinationsGenerator<String> combinationsGenerator;

    public ItemSetsGenerator(final int maxItemSetLength) {
        this.combinationsGenerator = new CombinationsGenerator<>(maxItemSetLength);
    }

    public Collection<ItemSetAndAssociation<String>> generateItemSetsAndAssociations(List<String> itemList,
                                                                                     int count) {
        Collection<Set<String>> combinations = combinationsGenerator.generateCombinations(itemList);
        Collection<ItemSetAndAssociation<String>> result = generateAssociations(combinations, itemList, count);
        return result;
    }

    private Collection<ItemSetAndAssociation<String>> generateAssociations(Collection<Set<String>> generated,
                                                                      List<String> transactionItems, long count) {
        Collection<ItemSetAndAssociation<String>> result = new HashSet<>();
        for (Set<String> combination : generated) {

            ItemSetAndAssociation<String> itemSetAndAssociation = new ItemSetAndAssociation<>();
            itemSetAndAssociation.setItemSet(generateItemSetRowKey(combination));
            itemSetAndAssociation.setCount(count);

            Map<String, Long> associationMap = new HashMap<>();
            for (String transactionItem : transactionItems) {
                if(!combination.contains(transactionItem)) {
                    associationMap.put(transactionItem, count);
                }
            }
            itemSetAndAssociation.setAssociationMap(associationMap);
            result.add(itemSetAndAssociation);
        }
        return result;
    }

    public List<String> generateCombinations(List<String> itemList) {
        Collection<Set<String>> combinations = combinationsGenerator.generateCombinations(itemList);
        return Lists.transform(new ArrayList<>(combinations), this::generateItemSetRowKey);
    }

    private String generateItemSetRowKey(Collection<String> itemSet) {
        StringBuilder result = new StringBuilder();

        List<String> sortedList = new ArrayList<>(itemSet);
        Collections.sort(sortedList);

        Iterator<String> itemSetIterator = itemSet.iterator();
        result.append(itemSetIterator.next());
        while (itemSetIterator.hasNext()){
            result.append("-").append(itemSetIterator.next());
        }
        return result.toString();
    }
}
