package com.my.sibyl.itemsets.dao;

import com.my.sibyl.itemsets.score_function.Recommendation;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.exceptions.HBaseException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author abykovsky
 * @since 1/22/15
 */
public interface ItemSetsDao {
    void updateItemSetCount(String instanceName, String itemSetRowKey, long count) throws IOException;

    long incrementItemSetCount(String instanceName, String itemSetRowKey, long count) throws IOException;

    void updateAssocCount(String instanceName, String itemSetRowKey, String itemIdColumnName, long count) throws IOException;

    long incrementAssocCount(String instanceName, String itemSetRowKey, String itemIdColumnName, long count) throws IOException;

    void incrementItemSetAndAssociations(String instanceName, String itemSetRowKey, long count, Map<String, Long> assocMap)
            throws IOException, HBaseException;

    void updateItemSetsCount(String instanceName, String itemSetRowKey, long count, Map<String, Long> assocMap) throws IOException, HBaseException;

    long getItemSetCount(String instanceName, String itemSetRowKey) throws IOException;

    long getItemSetCount(String instanceName, String itemSetRowKey, String itemIdColumnName) throws IOException;

    void getCountsForAssociations(String instanceName, List<Recommendation> recommendations) throws IOException;

    Map<String, Long> getItemSetsCount(String instanceName, Collection<String> itemSetRowKeys) throws IOException;

    Map<String, Long> getAssociations(String instanceName, String itemSetRowKey) throws IOException;

    void createTable(String instanceName) throws HBaseException, IOException;

    void deleteTable(String name) throws HBaseException, IOException;

    Map<String, Long> getItemSetWithCountMore(String instanceName, long count) throws IOException;
}
