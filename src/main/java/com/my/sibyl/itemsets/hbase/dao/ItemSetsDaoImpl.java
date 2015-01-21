package com.my.sibyl.itemsets.hbase.dao;

import com.my.sibyl.itemsets.dao.ItemSetsDao;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author abykovsky
 * @since 1/21/15
 */
public class ItemSetsDaoImpl implements ItemSetsDao {

    private static final byte[] TABLE_NAME = Bytes.toBytes("item_sets");
    private static final byte[] COUNT_FAM = Bytes.toBytes("C");
    private static final byte[] ASSOCIATION_FAM = Bytes.toBytes("A");
    private static final byte[] COUNT_COL = Bytes.toBytes("C");

    private HConnection connection;

    public ItemSetsDaoImpl(final HConnection connection) {
        this.connection = connection;
    }

    @Override
    public void updateCount(String itemSetRowKey, long count) throws IOException {
        Put p = makeUpdateCountPut(itemSetRowKey, count);

        try(HTableInterface itemSets = connection.getTable(TABLE_NAME)) {
            itemSets.put(p);
        }
    }

    @Override
    public long incrementCount(String itemSetRowKey, long count) throws IOException {
        try(HTableInterface itemSets = connection.getTable(TABLE_NAME)) {
            return itemSets.incrementColumnValue(Bytes.toBytes(itemSetRowKey), COUNT_FAM, COUNT_COL, count);
        }
    }

    private Put makeUpdateCountPut(String itemSetRowKey, long count) {
        Put p = new Put(Bytes.toBytes(itemSetRowKey));
        p.add(COUNT_FAM, COUNT_COL, Bytes.toBytes(count));
        return p;
    }

    @Override
    public void updateAssocCount(String itemSetRowKey, String itemIdColumnName, long count) throws IOException {
        Put p = makeUpdateAssocCountPut(itemSetRowKey, itemIdColumnName, count);

        try(HTableInterface itemSets = connection.getTable(TABLE_NAME)) {
            itemSets.put(p);
        }
    }

    @Override
    public long incrementAssocCount(String itemSetRowKey, String itemIdColumnName, long count) throws IOException {
        try(HTableInterface itemSets = connection.getTable(TABLE_NAME)) {
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
    public void updateCounts(String itemSetRowKey, long count, Map<String, Long> assocMap) throws IOException, HBaseException {

        List<Put> batch = new ArrayList<>();
        batch.add(makeUpdateCountPut(itemSetRowKey, count));

        for (Map.Entry<String, Long> entry : assocMap.entrySet()) {
            batch.add(makeUpdateAssocCountPut(itemSetRowKey, entry.getKey(), entry.getValue()));
        }

        Object[] results = new Object[batch.size()];

        try(HTableInterface itemSets = connection.getTable(TABLE_NAME)) {
            try {
                itemSets.batch(batch, results);
            } catch (InterruptedException e) {
                throw new HBaseException(e);
            }
        }
    }

    @Override
    public Long getCount(String itemSetRowKey) throws IOException {
        Get g = new Get(Bytes.toBytes(itemSetRowKey));
        g.addColumn(COUNT_FAM, COUNT_COL);

        try(HTableInterface itemSets = connection.getTable(TABLE_NAME)) {
            Result result = itemSets.get(g);
            Cell cell = result.getColumnLatestCell(COUNT_FAM, COUNT_COL);
            if(cell == null) return null;
            return Bytes.toLong(CellUtil.cloneValue(cell));
        }
    }

    @Override
    public Long getCount(String itemSetRowKey, String itemIdColumnName) throws IOException {
        Get g = new Get(Bytes.toBytes(itemSetRowKey));
        g.addColumn(ASSOCIATION_FAM, Bytes.toBytes(itemIdColumnName));

        try(HTableInterface itemSets = connection.getTable(TABLE_NAME)) {
            Result result = itemSets.get(g);
            Cell cell = result.getColumnLatestCell(ASSOCIATION_FAM, Bytes.toBytes(itemIdColumnName));
            if(cell == null) return null;
            return Bytes.toLong(CellUtil.cloneValue(cell));
        }
    }
}
