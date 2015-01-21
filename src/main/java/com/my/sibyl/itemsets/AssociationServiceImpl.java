package com.my.sibyl.itemsets;

import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl;
import com.my.sibyl.itemsets.util.PermutationsGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author abykovsky
 * @since 1/21/15
 */
public class AssociationServiceImpl {

    private static final Log LOG = LogFactory.getLog(AssociationServiceImpl.class);

    private int maxCombinationLength = (int) ConfigurationHolder.getConfiguration()
            .getInt("maxCombinationLength");

    private PermutationsGenerator<String> permutationsGenerator = new PermutationsGenerator<>(maxCombinationLength);

    private ItemSetsDao itemSetsDao;

    public AssociationServiceImpl(final HConnection connection) {
        itemSetsDao = new ItemSetsDaoImpl(connection);
    }

    public void processTransaction(List<String> transactionItems) throws IOException {
        processItemSets(transactionItems, 1);
    }

    private void processItemSets(List<String> transactionItems, int addAmount) throws IOException {
        Map<List<String>, Integer> generated = permutationsGenerator
                .generateItemSetsAndAssociations(transactionItems, addAmount);
        LOG.debug("Generated itemSets: " + generated);
        updateItemSets(generated);
    }

    private void updateItemSets(Map<List<String>, Integer> generated) throws IOException {

        for (Map.Entry<List<String>, Integer> entry : generated.entrySet()) {
            List<String> itemSet = entry.getKey();
            updateStoredItemSets(itemSet, entry.getValue());
        }
    }

    private long updateStoredItemSets(List<String> itemSet, int addAmount) throws IOException {
        if(itemSet.size() == 1) {
            return itemSetsDao.incrementCount(itemSet.iterator().next(), addAmount);
        } else {
            return itemSetsDao.incrementAssocCount(generateItemSetRowKey(itemSet.subList(0, itemSet.size() - 1)),
                    itemSet.get(itemSet.size() - 1),
                    addAmount);
        }
    }
    
    private String generateItemSetRowKey(List<String> itemSet) {
        StringBuilder result = new StringBuilder();

        Iterator<String> itemSetIterator = itemSet.iterator();
        result.append(itemSetIterator.next());
        while (itemSetIterator.hasNext()){
            result.append("-").append(itemSetIterator.next());
        }
        return result.toString();
    }
}
