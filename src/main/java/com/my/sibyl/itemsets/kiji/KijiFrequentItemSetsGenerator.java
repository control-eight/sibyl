package com.my.sibyl.itemsets.kiji;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 12/20/14
 */
public class KijiFrequentItemSetsGenerator {

    private static final int MAX_SET_SIZE = 2;

    private static final KijiDataRequest COUNT_REQUEST = KijiDataRequest.create(ItemSetsFields.INFO_FAMILY.getName(),
            ItemSetsFields.COUNT.getName());

    private final KijiTable kijiTable;

    private final KijiTableWriter kijiTableWriter;

    private final AtomicKijiPutter kijiPutter;

    private final KijiTableReader kijiTableReader;

    public KijiFrequentItemSetsGenerator(final KijiTable kijiTable, final KijiTableWriter kijiTableWriter,
                                         final AtomicKijiPutter kjiPutter, final KijiTableReader kijiTableReader) {
        this.kijiTable = kijiTable;
        this.kijiTableWriter = kijiTableWriter;
        this.kijiPutter = kjiPutter;
        this.kijiTableReader = kijiTableReader;
    }

    public Map<Set<Long>, Pair<Integer, Integer>> add(List<Long> transactionItems) {
        //long start = System.currentTimeMillis();
        Map<Set<Long>, Pair<Integer, Integer>> difference = addToItemSets(transactionItems, 1);
        //System.out.println("End " + (System.currentTimeMillis() - start) + "ms.");
        return difference;
    }

    public Map<Set<Long>, Pair<Integer, Integer>> remove(List<Long> transactionItems) {
        Map<Set<Long>, Pair<Integer, Integer>> difference = addToItemSets(transactionItems, -1);
        return difference;
    }

    private Map<Set<Long>, Pair<Integer, Integer>> addToItemSets(List<Long> transactionItems, int sign) {
        List<Set<Long>> itemSets = new ArrayList<>();
        List<Long> lastItemInItemSets = new ArrayList<>();
        for (Long item : transactionItems) {
            Set<Long> set = new HashSet<>();
            set.add(item);
            itemSets.add(set);
            lastItemInItemSets.add(item);
        }

        Set<Set<Long>> generated = new HashSet<>();
        generateItemSets(itemSets, lastItemInItemSets, transactionItems, generated);

        Map<Set<Long>, Pair<Integer, Integer>> difference = new HashMap<>();
        updateItemSets(generated, difference, sign);
        return difference;
    }

    private void updateItemSets(Set<Set<Long>> generated, Map<Set<Long>, Pair<Integer, Integer>> difference, int sign) {

        for (Set<Long> itemSet : generated) {
            final int freq = updateStoredItemSets(itemSet, sign);

            //itemsets size = 1 are used only to open new level when exceed level threshold
            //we need size = 1 to provide old lfs frequency
            //if(itemSet.size() > 1) {
            //Warning! It's not consistent safe mode but some error is acceptable here

            //left is old, right is new
                difference.put(itemSet, new ImmutablePair<>(freq - sign, freq));
            //}
        }
    }

    private void generateItemSets(List<Set<Long>> itemSets, List<Long> lastItemInItemSets,
                                  List<Long> transactionItems, Set<Set<Long>> generated) {
        for (int i = 0; i < itemSets.size(); i++) {
            Set<Long> itemSet = itemSets.get(i);

            generated.add(itemSet);

            if(itemSet.size() >= MAX_SET_SIZE) continue;

            List<Long> newLastItemInItemSets = new ArrayList<>();
            List<Set<Long>> nextLevelItemSets = generateNextLevel(itemSet, transactionItems,
                    lastItemInItemSets.get(i), newLastItemInItemSets);
            generateItemSets(nextLevelItemSets, newLastItemInItemSets, transactionItems, generated);
        }
    }

    private int updateStoredItemSets(Set<Long> itemSet, int sign) {
        try {
            EntityId itemSetId = kijiTable.getEntityId(format(itemSet));

            KijiCell<Long> result = kijiTableWriter.increment(itemSetId, ItemSetsFields.INFO_FAMILY.getName(),
                    ItemSetsFields.COUNT.getName(), sign);

            if(result.getData() < 0) {
                kijiPutter.begin(itemSetId);
                final long timestamp = System.currentTimeMillis();
                kijiPutter.put(ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.COUNT.getName(), timestamp, 0);
                kijiPutter.checkAndCommit(ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.COUNT.getName(), null);
                //we don't to repeat in failure commit case cause it means someone has already added count
                return 0;
            }
            return result.getData().intValue();

        } catch (KijiTableNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    String format(Set<Long> itemSet) {
        StringBuilder result = new StringBuilder();

        List<Long> itemList = new ArrayList<>();
        itemList.addAll(itemSet);
        Collections.sort(itemList);
        for (Long itemId : itemList) {
            result.append(itemId).append("-");
        }
        return result.substring(0, result.length() - 1);
    }

    //generate combinations for particular level
    private List<Set<Long>> generateNextLevel(Set<Long> curLevelItemSet, List<Long> transactionItems,
                                              Long lastItemInItemSet, List<Long> newLastItemInItemSets) {
        List<Set<Long>> result = new ArrayList<>();

        List<Long> nextLevelItemList = new ArrayList<>(curLevelItemSet);

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

        return result;
    }

    public Integer getCount(Set<Long> itemSet) throws IOException {
        if(itemSet == null || itemSet.isEmpty()) return 0;
        EntityId entityId = kijiTable.getEntityId(format(itemSet));
        return getCount(entityId);
    }

    public Integer getCount(Long item) throws IOException {
        if(item == null) return 0;
        EntityId entityId = kijiTable.getEntityId(item.toString());
        return getCount(entityId);
    }

    private Integer getCount(EntityId entityId) throws IOException {
        Long result = kijiTableReader.get(entityId, COUNT_REQUEST)
                .getMostRecentValue(ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.COUNT.getName());
        return result == null? 0: result.intValue();
    }
}
