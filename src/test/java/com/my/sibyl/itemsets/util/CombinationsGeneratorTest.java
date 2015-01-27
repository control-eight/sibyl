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

        Set itemSet1 = set(Arrays.asList("1"));
        Set itemSet2 = set(Arrays.asList("2"));
        Set itemSet12 = set(Arrays.asList("1", "2"));

        Collection<Set<String>> testResult = combinationsGenerator
                .generateCombinations(Arrays.asList("1", "2"));
        assertEquals("Size", 3, testResult.size());
        assertTrue("Contains 1", testResult.contains(itemSet1));
        assertTrue("Contains 2", testResult.contains(itemSet2));
        assertTrue("Contains 1-2", testResult.contains(itemSet12));
    }

    @Test
    public void testGenerateItemSetsAndAssociations3() {

        Set itemSet1 = set(Arrays.asList("1"));
        Set itemSet2 = set(Arrays.asList("2"));
        Set itemSet3 = set(Arrays.asList("3"));
        Set itemSet12 = set(Arrays.asList("1", "2"));
        Set itemSet13 = set(Arrays.asList("1", "3"));
        Set itemSet23 = set(Arrays.asList("2", "3"));

        Collection<Set<String>> testResult = combinationsGenerator
                .generateCombinations(Arrays.asList("1", "2", "3"));
        assertEquals("Size", 6, testResult.size());
        assertTrue("Contains 1", testResult.contains(itemSet1));
        assertTrue("Contains 2", testResult.contains(itemSet2));
        assertTrue("Contains 2", testResult.contains(itemSet3));
        assertTrue("Contains 1-2", testResult.contains(itemSet12));
        assertTrue("Contains 1-2", testResult.contains(itemSet13));
        assertTrue("Contains 1-2", testResult.contains(itemSet23));
    }

    @Test
    public void testGenerateItemSets3() {
        Collection<Set<String>> testResult = combinationsGenerator.generateCombinations(Arrays.asList("1", "2", "3"));
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
        Collection<Set<String>> testResult = combinationsGenerator.generateCombinations(Arrays.asList("1", "2", "3", "4"));
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

    public static ItemSetAndAssociation<String> createItemSetAndAssociation(String itemSet, long count, List<String> list2,
                                                             List<Long> list3) {
        ItemSetAndAssociation<String> result = new ItemSetAndAssociation<>();
        result.setItemSet(itemSet);
        result.setCount(count);
        if(list2 == null || list3 == null) {
            result.setAssociationMap(Collections.emptyMap());
        } else {
            result.setAssociationMap(map(list2, list3));
        }
        return result;
    }
}
