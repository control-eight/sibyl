package com.my.sibyl.itemsets.data_load.hadoop.transactions_dl.group;

import com.my.sibyl.itemsets.data_load.hadoop.transactions_dl.TransactionsDataLoadKVMapperGroupAbstract;
import com.my.sibyl.itemsets.data_load.hadoop.transactions_dl.group.dto.TransactionDto;
import com.my.sibyl.itemsets.model.Transaction;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collections;

/**
 * HBase bulk import example
 * <p>
 * Parses Facebook and Twitter messages from CSV files and outputs
 * <ImmutableBytesWritable, Put>.
 *
 * @author abykovsky
 * @since 1/29/15
 */
public class TransactionsDataLoadMapper extends TransactionsDataLoadKVMapperGroupAbstract {

    private final static int NUM_FIELDS = 8;

    @Override
    protected boolean isHeader(Text value) {
        return value.find("itemID") > -1;
    }

    @Override
    protected int getNumFields() {
        return NUM_FIELDS;
    }

    @Override
    protected String getClassName() {
        return "TransactionsDataLoadMapperGroup";
    }

    @Override
    protected String[] parseLine(Text value) throws IOException {
        return value.toString().split("\\t");
    }

    protected TransactionDto createTransaction(String fields[], Context context) {
        //product_id
        String itemId = fields[1];
        if ("0".equals(itemId)) {
            context.getCounter(getClassName(), "PRODUCT_ID_IS_ZERO").increment(1);
            return null;
        }

        TransactionDto transaction = new TransactionDto();
        //orderID
        transaction.setId(fields[2]);
        transaction.setItem(itemId);
        //Quantity
        transaction.setQuantity(Integer.parseInt(fields[5]));
        transaction.setCreateTimestamp(-1l);
        return transaction;
    }
}