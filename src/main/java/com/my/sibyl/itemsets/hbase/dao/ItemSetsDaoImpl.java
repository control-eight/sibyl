package com.my.sibyl.itemsets.hbase.dao;

import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.score_function.Recommendation;
import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author abykovsky
 * @since 1/21/15
 */
public class ItemSetsDaoImpl implements ItemSetsDao {

    public static final String TABLE_NAME = "item_sets";
    //public static final byte[] TABLE_NAME = Bytes.toBytes("item_sets");
    public static final byte[] COUNT_FAM = Bytes.toBytes("C");
    public static final byte[] ASSOCIATION_FAM = Bytes.toBytes("A");
    public static final byte[] COUNT_COL = Bytes.toBytes("C");

    private HConnection connection;

    public ItemSetsDaoImpl(final HConnection connection) {
        this.connection = connection;
    }

    @Override
    public void updateItemSetCount(String instanceName, String itemSetRowKey, long count) throws IOException {
        Put p = makeUpdateCountPut(itemSetRowKey, count);

        try(HTableInterface itemSets = connection.getTable(getTableName(instanceName))) {
            itemSets.put(p);
        }
    }

    private String getTableName(String instanceName) {
        return TABLE_NAME + "_" + instanceName;
    }

    @Override
    public long incrementItemSetCount(String instanceName, String itemSetRowKey, long count) throws IOException {
        try(HTableInterface itemSets = connection.getTable(getTableName(instanceName))) {
            return itemSets.incrementColumnValue(Bytes.toBytes(itemSetRowKey), COUNT_FAM, COUNT_COL, count);
        }
    }

    private Put makeUpdateCountPut(String itemSetRowKey, long count) {
        Put p = new Put(Bytes.toBytes(itemSetRowKey));
        p.add(COUNT_FAM, COUNT_COL, Bytes.toBytes(count));
        return p;
    }

    @Override
    public void updateAssocCount(String instanceName, String itemSetRowKey, String itemIdColumnName, long count) throws IOException {
        Put p = makeUpdateAssocCountPut(itemSetRowKey, itemIdColumnName, count);

        try(HTableInterface itemSets = connection.getTable(getTableName(instanceName))) {
            itemSets.put(p);
        }
    }

    @Override
    public long incrementAssocCount(String instanceName, String itemSetRowKey, String itemIdColumnName, long count) throws IOException {
        try(HTableInterface itemSets = connection.getTable(getTableName(instanceName))) {
            return itemSets.incrementColumnValue(Bytes.toBytes(itemSetRowKey), ASSOCIATION_FAM,
                    Bytes.toBytes(itemIdColumnName), count);
        }
    }

    private Put makeUpdateAssocCountPut(String itemSetRowKey, String itemIdColumnName, long count) {
        Put p = new Put(Bytes.toBytes(itemSetRowKey));
        p.add(ASSOCIATION_FAM, Bytes.toBytes(itemIdColumnName), Bytes.toBytes(count));
        return p;
    }

    @Override
    public void incrementItemSetAndAssociations(String instanceName, String itemSetRowKey, long count, Map<String, Long> assocMap)
            throws IOException, HBaseException {
        List<Increment> batch = new ArrayList<>();
        byte[] row = Bytes.toBytes(itemSetRowKey);
        Increment increment = new Increment(row);
        increment.addColumn(COUNT_FAM, COUNT_COL, count);
        batch.add(increment);

        for (Map.Entry<String, Long> entry : assocMap.entrySet()) {
            increment = new Increment(row);
            increment.addColumn(ASSOCIATION_FAM, Bytes.toBytes(entry.getKey()), entry.getValue());
            batch.add(increment);
        }

        Object[] results = new Object[batch.size()];

        try(HTableInterface itemSets = connection.getTable(getTableName(instanceName))) {
            try {
                itemSets.batch(batch, results);
            } catch (InterruptedException e) {
                throw new HBaseException(e);
            }
        }
    }

    @Override
    public void updateItemSetsCount(String instanceName, String itemSetRowKey, long count, Map<String, Long> assocMap) throws IOException, HBaseException {

        List<Put> batch = new ArrayList<>();
        batch.add(makeUpdateCountPut(itemSetRowKey, count));

        for (Map.Entry<String, Long> entry : assocMap.entrySet()) {
            batch.add(makeUpdateAssocCountPut(itemSetRowKey, entry.getKey(), entry.getValue()));
        }

        Object[] results = new Object[batch.size()];

        try(HTableInterface itemSets = connection.getTable(getTableName(instanceName))) {
            try {
                itemSets.batch(batch, results);
            } catch (InterruptedException e) {
                throw new HBaseException(e);
            }
        }
    }

    @Override
    public Long getItemSetCount(String instanceName, String itemSetRowKey) throws IOException {
        Get g = new Get(Bytes.toBytes(itemSetRowKey));
        g.addColumn(COUNT_FAM, COUNT_COL);

        try(HTableInterface itemSets = connection.getTable(getTableName(instanceName))) {
            Result result = itemSets.get(g);
            byte[] value = result.getValue(COUNT_FAM, COUNT_COL);
            if(value == null) return null;
            return Bytes.toLong(value);
        }
    }

    @Override
    public Long getItemSetCount(String instanceName, String itemSetRowKey, String itemIdColumnName) throws IOException {
        Get g = new Get(Bytes.toBytes(itemSetRowKey));
        g.addColumn(ASSOCIATION_FAM, Bytes.toBytes(itemIdColumnName));

        try(HTableInterface itemSets = connection.getTable(getTableName(instanceName))) {
            Result result = itemSets.get(g);
            byte[] value = result.getValue(ASSOCIATION_FAM, Bytes.toBytes(itemIdColumnName));
            if(value == null) return null;
            return Bytes.toLong(value);
        }
    }

    @Override
    public void getCountsForAssociations(String instanceName, List<Recommendation> recommendations) throws IOException {

        List<Get> batch = new ArrayList<>();

        for (Recommendation recommendation : recommendations) {
            Get get = new Get(Bytes.toBytes(recommendation.getAssociationId()));
            get.addColumn(COUNT_FAM, COUNT_COL);
            batch.add(get);
        }

        try(HTableInterface itemSets = connection.getTable(getTableName(instanceName))) {
            Result[] results = itemSets.get(batch);
            for (int i = 0; i < recommendations.size(); i++) {
                byte[] value = results[i].getValue(COUNT_FAM, COUNT_COL);
                if(value != null) {
                    recommendations.get(i).setCountOfAssociationAsItemSet(Bytes.toLong(value));
                }
            }
        }
    }

    @Override
    public Map<String, Long> getItemSetsCount(String instanceName, Collection<String> itemSetRowKeys) throws IOException {

        List<Get> batch = new ArrayList<>();

        for (String itemSetRowKey : itemSetRowKeys) {
            Get get = new Get(Bytes.toBytes(itemSetRowKey));
            get.addColumn(COUNT_FAM, COUNT_COL);
            batch.add(get);
        }

        try(HTableInterface itemSets = connection.getTable(getTableName(instanceName))) {
            Result[] results = itemSets.get(batch);
            Map<String, Long> result = new HashMap<>();
            for (int i = 0; i < itemSetRowKeys.size(); i++) {
                byte[] value = results[i].getValue(COUNT_FAM, COUNT_COL);
                if(value != null) {
                    result.put(Bytes.toString(results[i].getRow()), Bytes.toLong(value));
                } else {
                    result.put(Bytes.toString(results[i].getRow()), null);
                }
            }
            return result;
        }
    }

    @Override
    public Map<String, Long> getAssociations(String instanceName, String itemSetRowKey) throws IOException {
        Get g = new Get(Bytes.toBytes(itemSetRowKey));
        g.addFamily(ASSOCIATION_FAM);

        Map<String, Long> associationMap = new HashMap<>();
        try(HTableInterface itemSets = connection.getTable(getTableName(instanceName))) {
            Result result = itemSets.get(g);
            if(result == null) return Collections.emptyMap();
            for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(ASSOCIATION_FAM).entrySet()) {
                associationMap.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
            }
        }
        return associationMap;
    }
}
