package com.my.sibyl.itemsets.hbase.dao;

import com.my.sibyl.itemsets.model.Transaction;
import com.my.sibyl.itemsets.util.Avro;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/29/15
 */
public class TransactionsDaoImpl implements com.my.sibyl.itemsets.dao.TransactionsDao {

    public static final byte[] TABLE_NAME = Bytes.toBytes("transactions");
    public static final byte[] INFO_FAM = Bytes.toBytes("I");
    public static final byte[] ITEMS_COLUMN = Bytes.toBytes("T");

    private HConnection connection;

    public TransactionsDaoImpl(HConnection connection) {
        this.connection = connection;
    }

    @Override
    public List<Transaction> scanTransactions(Date startRow, Date stopRow) throws IOException {
        Scan scan = new Scan(Bytes.toBytes(startRow.getTime() + ""), Bytes.toBytes(stopRow.getTime() + ""));
        scan.addColumn(INFO_FAM, ITEMS_COLUMN);
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
}
