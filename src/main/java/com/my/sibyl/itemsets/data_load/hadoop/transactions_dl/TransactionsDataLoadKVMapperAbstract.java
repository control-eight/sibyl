package com.my.sibyl.itemsets.data_load.hadoop.transactions_dl;

import com.my.sibyl.itemsets.hbase.dao.TransactionsDaoImpl;
import com.my.sibyl.itemsets.model.Transaction;
import com.my.sibyl.itemsets.util.Avro;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author abykovsky
 * @since 6/18/15
 */
public abstract class TransactionsDataLoadKVMapperAbstract extends
        Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    String tableName = "";

    ImmutableBytesWritable hKey = new ImmutableBytesWritable();

    /** {@inheritDoc} */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        Configuration c = context.getConfiguration();

        tableName = c.get("hbase.table.name");
    }

    /** {@inheritDoc} */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (isHeader(value)) {
            // Skip header
            return;
        }

        String[] fields;

        try {
            fields = parseLine(value);
        } catch (Exception ex) {
            context.getCounter(getClassName(), "PARSE_ERRORS").increment(1);
            return;
        }

        if (fields.length != getNumFields()) {
            context.getCounter(getClassName(), "INVALID_FIELD_LEN").increment(1);
            return;
        }

        Transaction transaction;
        try {
            transaction = createTransaction(fields, context);
        } catch (NumberFormatException e) {
            context.getCounter(getClassName(), "INVALID_NUMBER").increment(1);
            return;
        }

        if(transaction == null) return;

        // Key: e.g. ":"
        hKey.set(TransactionsDaoImpl.createRowKey(transaction));

        if (!transaction.getItems().isEmpty()) {
            Put put = new Put(hKey.get());
            put.add(TransactionsDaoImpl.INFO_FAM, TransactionsDaoImpl.ITEMS_COLUMN, Avro.transactionToBytes(transaction));
            context.write(hKey, put);
        }

        context.getCounter(getClassName(), "NUM_MSGS").increment(1);
    }

    protected abstract String[] parseLine(Text value) throws IOException;

    protected abstract boolean isHeader(Text value);

    protected abstract int getNumFields();

    protected abstract String getClassName();

    protected abstract Transaction createTransaction(String fields[], Context orderDate);
}
