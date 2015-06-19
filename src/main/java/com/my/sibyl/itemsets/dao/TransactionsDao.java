package com.my.sibyl.itemsets.dao;

import com.my.sibyl.itemsets.model.Transaction;
import org.apache.hadoop.hbase.exceptions.HBaseException;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/29/15
 */
public interface TransactionsDao {

    List<Transaction> scanTransactions(String instanceName, long startRow, long stopRow) throws IOException;

    List<Transaction> scanTransactions(String instanceName, Date startRow, Date stopRow) throws IOException;

    void addTransaction(String instanceName, Transaction transaction) throws IOException;

    void createTable(String instanceName) throws HBaseException, IOException;

    void deleteTable(String name) throws HBaseException, IOException;
}
