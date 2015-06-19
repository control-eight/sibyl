package com.my.sibyl.itemsets.hbase.dao;

import com.my.sibyl.itemsets.InstancesService;
import com.my.sibyl.itemsets.model.Transaction;
import com.my.sibyl.itemsets.util.Avro;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.util.Bytes;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/29/15
 */
@Singleton
public class TransactionsDaoImpl implements com.my.sibyl.itemsets.dao.TransactionsDao {

    private static final Log LOG = LogFactory.getLog(TransactionsDaoImpl.class);

    public static final String TABLE_NAME = "transactions";
    //public static final byte[] TABLE_NAME = Bytes.toBytes(TABLE_NAME_STRING);
    public static final byte[] INFO_FAM = Bytes.toBytes("I");
    public static final byte[] ITEMS_COLUMN = Bytes.toBytes("T");

    @Inject
    private HConnection connection;

    public TransactionsDaoImpl() {
    }

    public TransactionsDaoImpl(HConnection connection) {
        this.connection = connection;
    }

    @Override
    public List<Transaction> scanTransactions(String instanceName, long startRow, long stopRow) throws IOException {
        return scanTransactions(instanceName, new Date(startRow), new Date(stopRow));
    }

    @Override
    public List<Transaction> scanTransactions(String instanceName, Date startRow, Date stopRow) throws IOException {
        Scan scan = makeTransactionsScan(startRow, stopRow);
        try(HTableInterface transactions = connection.getTable(getTableName(instanceName))) {
            ResultScanner resultScanner = transactions.getScanner(scan);
            List<Transaction> transactionList = new ArrayList<>();
            for (Result result : resultScanner) {
                byte[] value = result.getValue(INFO_FAM, ITEMS_COLUMN);
                if(value != null) {
                    transactionList.add(Avro.bytesToTransaction(value));
                }
            }
            return transactionList;
        }
    }

    @Override
    public void addTransaction(String instanceName, Transaction transaction) throws IOException {
        Put p = new Put(createRowKey(transaction));
        p.add(INFO_FAM, ITEMS_COLUMN, Avro.transactionToBytes(transaction));
        try(HTableInterface transactions = connection.getTable(getTableName(instanceName))) {
            transactions.put(p);
        }
    }

    public static Scan makeTransactionsScan(Date startRow, Date stopRow) {
        return makeTransactionsScan(startRow.getTime(), stopRow.getTime());
    }

    public static Scan makeTransactionsScan(Long startRow, Long stopRow) {
        Scan scan;
        if(startRow == -1) {
            scan = new Scan();
            LOG.warn("Full scan is created");
        } else if(stopRow == -1) {
            scan = new Scan(Bytes.toBytes(startRow + ""));
        } else {
            scan = new Scan(Bytes.toBytes(startRow + ""), Bytes.toBytes(stopRow + ""));
        }
        scan.addColumn(INFO_FAM, ITEMS_COLUMN);
        return scan;
    }

    public static byte[] createRowKey(Transaction transaction) {
        return createRowKey(transaction.getCreateTimestamp(), transaction.getId());
    }

    public static byte[] createRowKey(long timestamp, long id) {
        return createRowKey(timestamp, id);
    }

    public static byte[] createRowKey(long transactionCreateTimestamp, String transactionId) {
        return String.format("%s:%s", transactionCreateTimestamp, transactionId).getBytes();
    }

    @Override
    public void createTable(String instanceName) throws HBaseException, IOException {
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(connection);

            HTableDescriptor defaultItemSetsDescriptor;
            try(HTableInterface itemSets = connection.getTable(getTableName(InstancesService.DEFAULT))) {
                defaultItemSetsDescriptor = itemSets.getTableDescriptor();
            }

            defaultItemSetsDescriptor.setName(Bytes.toBytes(getTableName(instanceName)));
            HTableDescriptor instanceItemSetsDescriptor = new HTableDescriptor(defaultItemSetsDescriptor);
            hBaseAdmin.createTable(instanceItemSetsDescriptor);

        } catch (TableExistsException e) {
            LOG.warn("Table " + getTableName(instanceName) + " is already exist. Skip creation");
        } catch (MasterNotRunningException | ZooKeeperConnectionException e) {
            throw new HBaseException(e);
        }
    }

    @Override
    public void deleteTable(String name) throws HBaseException, IOException {
        throw new UnsupportedOperationException();
    }

    public static String getTableName(String instanceName) {
        return TABLE_NAME + "_" + instanceName;
    }
}
