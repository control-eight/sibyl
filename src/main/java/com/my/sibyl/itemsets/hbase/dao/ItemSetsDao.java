package com.my.sibyl.itemsets.hbase.dao;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author abykovsky
 * @since 1/21/15
 */
public class ItemSetsDao {

    private static final byte[] TABLE_NAME = Bytes.toBytes("item_sets");
    private static final byte[] COUNT_FAM = Bytes.toBytes("C");
    private static final byte[] ASSOCIATION_FAM = Bytes.toBytes("A");
    private static final byte[] COUNT_COL = Bytes.toBytes("C");

    private HConnection connection;

    public ItemSetsDao(final HConnection connection) {
        this.connection = connection;
    }

    public void updateCount(String itemSetRowKey, Integer count) throws IOException {
        Put p = new Put(Bytes.toBytes(itemSetRowKey));
        p.add(COUNT_FAM, COUNT_COL, Bytes.toBytes(count));

        try(HTableInterface itemSets = connection.getTable(TABLE_NAME)) {
            itemSets.put(p);
        }
    }

    public void updateCount(String itemSetRowKey, String itemIdColumnName, Integer count) throws IOException {
        Put p = new Put(Bytes.toBytes(itemSetRowKey));
        p.add(ASSOCIATION_FAM, Bytes.toBytes(itemIdColumnName), Bytes.toBytes(count));

        try(HTableInterface itemSets = connection.getTable(TABLE_NAME)) {
            itemSets.put(p);
        }
    }
}
