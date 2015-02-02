package com.my.sibyl.itemsets.dao;

import com.my.sibyl.itemsets.model.Transaction;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/29/15
 */
public interface TransactionsDao {
    List<Transaction> scanTransactions(Date startRow, Date stopRow) throws IOException;

    void addTransaction(Transaction transaction) throws IOException;
}
