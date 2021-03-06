package com.my.sibyl.itemsets.util;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by dkopiychenko on 1/28/15.
 */
public class AssociationsGenerator {
    // Maximum item number in itemset associations generated for. MaxItemsNumber > 0.
    private int MaxItemsNumber;

    //TODO add Service Property
    public AssociationsGenerator() {
        MaxItemsNumber = 2;
    }

    public AssociationsGenerator(int maxItemsNumber) {
        MaxItemsNumber = maxItemsNumber;
    }

    public List<Associations> generateAssociations(List<String> transaction) {
        Collections.sort(transaction);
        List<Associations> result = new ArrayList<>();
        for (int i = 0; i < transaction.size(); i++) {
            result.addAll(generateAssociations(new ArrayList<>(), transaction.get(i), transaction.subList(0, i), transaction.subList(i + 1, transaction.size())));
        }
        return result;
    }

    public List<Associations> generateAssociations(List<String> prefix, String lastElement, List<String> left, List<String> right) {
        List<Associations> result = new ArrayList<>();
        List<String> itemset = new ArrayList<>(prefix);
        itemset.add(lastElement);
        List<String> associations = new ArrayList<>(left);
        associations.addAll(right);
        result.add(new Associations(itemset, associations));

        if (itemset.size() < MaxItemsNumber) {
            for (int i = 0; i < right.size(); i++) {
                List<String> newLeft = new ArrayList<String>(left);
                newLeft.addAll(right.subList(0, i));
                result.addAll(generateAssociations(itemset, right.get(i), newLeft, right.subList(i + 1, right.size())));

            }

        }

        return result;
    }

    public List<List<String>> generateItemsets(List<String> transaction) {
        Collections.sort(transaction);
        List<List<String>> result = new ArrayList<>();
        for (int i = 0; i < transaction.size(); i++) {
            result.addAll(generateItemsets(new ArrayList<>(), transaction.get(i), transaction.subList(i + 1, transaction.size())));
        }
        return result;
    }

    public List<List<String>> generateItemsets(List<String> prefix, String lastElement, List<String> right) {
        List<List<String>> result = new ArrayList<>();
        List<String> itemset = new ArrayList<>(prefix);
        itemset.add(lastElement);
        result.add(itemset);

        if (itemset.size() < MaxItemsNumber) {
            for (int i = 0; i < right.size(); i++) {
                result.addAll(generateItemsets(itemset, right.get(i), right.subList(i + 1, right.size())));

            }

        }

        return result;
    }
}
