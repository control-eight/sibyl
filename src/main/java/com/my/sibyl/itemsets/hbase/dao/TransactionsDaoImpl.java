package com.my.sibyl.itemsets.hbase.dao;

import com.my.sibyl.itemsets.model.Transaction;
import com.my.sibyl.itemsets.util.Avro;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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

    public static final String TABLE_NAME_STRING = "transactions";
    public static final byte[] TABLE_NAME = Bytes.toBytes(TABLE_NAME_STRING);
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
    public List<Transaction> scanTransactions(Date startRow, Date stopRow) throws IOException {
        Scan scan = makeTransactionsScan(startRow, stopRow);
        try(HTableInterface transactions = connection.getTable(TABLE_NAME)) {
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
    public void addTransaction(Transaction transaction) throws IOException {
        Put p = new Put(createRowKey(transaction));
        p.add(INFO_FAM, ITEMS_COLUMN, Avro.transactionToBytes(transaction));
        try(HTableInterface transactions = connection.getTable(TABLE_NAME)) {
            transactions.put(p);
        }
    }

    public static Scan makeTransactionsScan(Date startRow, Date stopRow) {
        return makeTransactionsScan(startRow.getTime(), stopRow.getTime());
    }

    public static Scan makeTransactionsScan(Long startRow, Long stopRow) {
        Scan scan = new Scan(Bytes.toBytes(startRow + ""), Bytes.toBytes(stopRow + ""));
        scan.addColumn(INFO_FAM, ITEMS_COLUMN);
        return scan;
    }

    public static byte[] createRowKey(Transaction transaction) {
        return createRowKey(transaction.getCreateTimestamp(), transaction.getId());
    }

    public static byte[] createRowKey(long transactionCreateTimestamp, String transactionId) {
        return String.format("%s:%s", transactionCreateTimestamp, transactionId).getBytes();
    }
}
