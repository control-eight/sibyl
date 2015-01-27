package com.my.sibyl.itemsets.util;

import com.my.sibyl.itemsets.ItemSetAndAssociation;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static com.my.sibyl.itemsets.util.CombinationsGeneratorTest.*;

/**
 * @author abykovsky
 * @since 1/27/15
 */
public class ItemSetsGeneratorTest {

    private int maxItemSetLength = 2;

    private ItemSetsGenerator itemSetsGenerator = new ItemSetsGenerator(maxItemSetLength);

    @Test
    public void testGenerateItemSetsAndAssociations2() {

        ItemSetAndAssociation<String> itemSetAndAssociation1 = createItemSetAndAssociation("1", 1l,
                Arrays.asList("2"), Arrays.asList(1l));
        ItemSetAndAssociation<String> itemSetAndAssociation2 = createItemSetAndAssociation("2", 1l,
                Arrays.asList("1"), Arrays.asList(1l));
        ItemSetAndAssociation<String> itemSetAndAssociation12 = createItemSetAndAssociation("1-2", 1l,
                null, null);

        Collection<ItemSetAndAssociation<String>> testResult = itemSetsGenerator
                .generateItemSetsAndAssociations(Arrays.asList("1", "2"), 1);
        assertEquals("Size", 3, testResult.size());
        assertTrue("Contains 1", testResult.contains(itemSetAndAssociation1));
        assertTrue("Contains 2", testResult.contains(itemSetAndAssociation2));
        assertTrue("Contains 1-2", testResult.contains(itemSetAndAssociation12));
    }

    @Test
    public void testGenerateItemSetsAndAssociations3() {

        ItemSetAndAssociation<String> itemSetAndAssociation1 = createItemSetAndAssociation("1", 1l,
                Arrays.asList("2", "3"), Arrays.asList(1l, 1l));
        ItemSetAndAssociation<String> itemSetAndAssociation2 = createItemSetAndAssociation("2", 1l,
                Arrays.asList("1", "3"), Arrays.asList(1l, 1l));
        ItemSetAndAssociation<String> itemSetAndAssociation3 = createItemSetAndAssociation("3", 1l,
                Arrays.asList("1", "2"), Arrays.asList(1l, 1l));
        ItemSetAndAssociation<String> itemSetAndAssociation12 = createItemSetAndAssociation("1-2", 1l,
                Arrays.asList("3"), Arrays.asList(1l));
        ItemSetAndAssociation<String> itemSetAndAssociation13 = createItemSetAndAssociation("1-3", 1l,
                Arrays.asList("2"), Arrays.asList(1l));
        ItemSetAndAssociation<String> itemSetAndAssociation23 = createItemSetAndAssociation("2-3", 1l,
                Arrays.asList("1"), Arrays.asList(1l));

        Collection<ItemSetAndAssociation<String>> testResult = itemSetsGenerator
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
        List<String> testResult = itemSetsGenerator.generateCombinations(Arrays.asList("1", "2", "3"));
        assertEquals("Size", 6, testResult.size());
        assertTrue("Contains 1", testResult.contains("1"));
        assertTrue("Contains 2", testResult.contains("2"));
        assertTrue("Contains 3", testResult.contains("3"));
        assertTrue("Contains 1-2", testResult.contains("1-2"));
        assertTrue("Contains 1-3", testResult.contains("1-3"));
        assertTrue("Contains 2-3", testResult.contains("2-3"));
    }

    @Test
    public void testGenerateItemSets4() {
        List<String> testResult = itemSetsGenerator.generateCombinations(Arrays.asList("1", "2", "3", "4"));
        assertEquals("Size", 10, testResult.size());
        assertTrue("Contains 1", testResult.contains("1"));
        assertTrue("Contains 2", testResult.contains("2"));
        assertTrue("Contains 3", testResult.contains("3"));
        assertTrue("Contains 4", testResult.contains("4"));
        assertTrue("Contains 1-2", testResult.contains("1-2"));
        assertTrue("Contains 1-3", testResult.contains("1-3"));
        assertTrue("Contains 1-4", testResult.contains("1-4"));
        assertTrue("Contains 2-3", testResult.contains("2-3"));
        assertTrue("Contains 2-4", testResult.contains("2-4"));
        assertTrue("Contains 3-4", testResult.contains("3-4"));
    }
}
