package com.my.sibyl.itemsets.dao;

import org.apache.hadoop.hbase.exceptions.HBaseException;

import java.io.IOException;
import java.util.Map;

/**
 * @author abykovsky
 * @since 1/22/15
 */
public interface ItemSetsDao {
    void updateCount(String itemSetRowKey, long count) throws IOException;

    long incrementCount(String itemSetRowKey, long count) throws IOException;

    void updateAssocCount(String itemSetRowKey, String itemIdColumnName, long count) throws IOException;

    long incrementAssocCount(String itemSetRowKey, String itemIdColumnName, long count) throws IOException;

    void updateCounts(String itemSetRowKey, long count, Map<String, Long> assocMap) throws IOException, HBaseException;

    Long getCount(String itemSetRowKey) throws IOException;

    Long getCount(String itemSetRowKey, String itemIdColumnName) throws IOException;
}
