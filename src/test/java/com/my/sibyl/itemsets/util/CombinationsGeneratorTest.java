package com.my.sibyl.itemsets.util;

import com.my.sibyl.itemsets.ItemSetAndAssociation;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author abykovsky
 * @since 1/25/15
 */
public class CombinationsGeneratorTest {

    private int maxItemSetLength = 2;

    private CombinationsGenerator<String> combinationsGenerator = new CombinationsGenerator<>(maxItemSetLength);

    @Test
    public void testGenerateItemSetsAndAssociations2() {

        ItemSetAndAssociation<String> itemSetAndAssociation1 = createItemSetAndAssociation(Arrays.asList("1"), 1l,
                Arrays.asList("2"), Arrays.asList(1l));
        ItemSetAndAssociation<String> itemSetAndAssociation2 = createItemSetAndAssociation(Arrays.asList("2"), 1l,
                Arrays.asList("1"), Arrays.asList(1l));
        ItemSetAndAssociation<String> itemSetAndAssociation12 = createItemSetAndAssociation(Arrays.asList("1", "2"), 1l,
                null, null);

        Collection<ItemSetAndAssociation<String>> testResult = combinationsGenerator
                .generateItemSetsAndAssociations(Arrays.asList("1", "2"), 1);
        assertEquals("Size", 3, testResult.size());
        assertTrue("Contains 1", testResult.contains(itemSetAndAssociation1));
        assertTrue("Contains 2", testResult.contains(itemSetAndAssociation2));
        assertTrue("Contains 1-2", testResult.contains(itemSetAndAssociation12));
    }

    @Test
    public void testGenerateItemSetsAndAssociations3() {

        ItemSetAndAssociation<String> itemSetAndAssociation1 = createItemSetAndAssociation(Arrays.asList("1"), 1l,
                Arrays.asList("2", "3"), Arrays.asList(1l, 1l));
        ItemSetAndAssociation<String> itemSetAndAssociation2 = createItemSetAndAssociation(Arrays.asList("2"), 1l,
                Arrays.asList("1", "3"), Arrays.asList(1l, 1l));
        ItemSetAndAssociation<String> itemSetAndAssociation3 = createItemSetAndAssociation(Arrays.asList("3"), 1l,
                Arrays.asList("1", "2"), Arrays.asList(1l, 1l));
        ItemSetAndAssociation<String> itemSetAndAssociation12 = createItemSetAndAssociation(Arrays.asList("1", "2"), 1l,
                Arrays.asList("3"), Arrays.asList(1l));
        ItemSetAndAssociation<String> itemSetAndAssociation13 = createItemSetAndAssociation(Arrays.asList("1", "3"), 1l,
                Arrays.asList("2"), Arrays.asList(1l));
        ItemSetAndAssociation<String> itemSetAndAssociation23 = createItemSetAndAssociation(Arrays.asList("2", "3"), 1l,
                Arrays.asList("1"), Arrays.asList(1l));

        Collection<ItemSetAndAssociation<String>> testResult = combinationsGenerator
                .generateItemSetsAndAssociations(Arrays.asList("1", "2", "3"), 1);
        assertEquals("Size", 6, testResult.size());
        assertTrue("Contains 1", testResult.contains(itemSetAndAssociation1));
        assertTrue("Contains 2", testResult.contains(itemSetAndAssociation2));
        assertTrue("Contains 2", testResult.contains(itemSetAndAssociation3));
        assertTrue("Contains 1-2", testResult.contains(itemSetAndAssociation12));
        assertTrue("Contains 1-2", testResult.contains(itemSetAndAssociation13));
        assertTrue("Contains 1-2", testResult.contains(itemSetAndAssociation23));
    }

    @Test
    public void testGenerateItemSets3() {
        Collection<Set<String>> testResult = combinationsGenerator.generateItemSets(Arrays.asList("1", "2", "3"));
        assertEquals("Size", 6, testResult.size());
        assertTrue("Contains 1", testResult.contains(set(Arrays.asList("1"))));
        assertTrue("Contains 2", testResult.contains(set(Arrays.asList("2"))));
        assertTrue("Contains 3", testResult.contains(set(Arrays.asList("3"))));
        assertTrue("Contains 1-2", testResult.contains(set(Arrays.asList("1", "2"))));
        assertTrue("Contains 1-3", testResult.contains(set(Arrays.asList("1", "3"))));
        assertTrue("Contains 2-3", testResult.contains(set(Arrays.asList("2", "3"))));
    }

    @Test
    public void testGenerateItemSets4() {
        Collection<Set<String>> testResult = combinationsGenerator.generateItemSets(Arrays.asList("1", "2", "3", "4"));
        assertEquals("Size", 10, testResult.size());
        assertTrue("Contains 1", testResult.contains(set(Arrays.asList("1"))));
        assertTrue("Contains 2", testResult.contains(set(Arrays.asList("2"))));
        assertTrue("Contains 3", testResult.contains(set(Arrays.asList("3"))));
        assertTrue("Contains 4", testResult.contains(set(Arrays.asList("4"))));
        assertTrue("Contains 1-2", testResult.contains(set(Arrays.asList("1", "2"))));
        assertTrue("Contains 1-3", testResult.contains(set(Arrays.asList("1", "3"))));
        assertTrue("Contains 1-4", testResult.contains(set(Arrays.asList("1", "4"))));
        assertTrue("Contains 2-3", testResult.contains(set(Arrays.asList("2", "3"))));
        assertTrue("Contains 2-4", testResult.contains(set(Arrays.asList("2", "4"))));
        assertTrue("Contains 3-4", testResult.contains(set(Arrays.asList("3", "4"))));
    }

    public static <T> Set<T> set(List<T> list) {
        return new HashSet<>(list);
    }

    public static <K, V> Map<K, V> map(List<K> keyList, List<V> valueList) {
        Map<K, V> result = new HashMap<>();
        for (int i = 0; i < keyList.size(); i++) {
            result.put(keyList.get(i), valueList.get(i));
        }
        return result;
    }

    public static ItemSetAndAssociation<String> createItemSetAndAssociation(List<String> list, long count, List<String> list2,
                                                             List<Long> list3) {
        ItemSetAndAssociation<String> result = new ItemSetAndAssociation<>();
        result.setItemSet(set(list));
        result.setCount(count);
        if(list2 == null || list3 == null) {
            result.setAssociationMap(Collections.emptyMap());
        } else {
            result.setAssociationMap(map(list2, list3));
        }
        return result;
    }
}
