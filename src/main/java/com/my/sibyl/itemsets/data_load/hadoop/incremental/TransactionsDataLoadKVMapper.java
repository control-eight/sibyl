package com.my.sibyl.itemsets.data_load.hadoop.incremental;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.my.sibyl.itemsets.hbase.dao.TransactionsDaoImpl;
import com.my.sibyl.itemsets.model.Transaction;
import com.my.sibyl.itemsets.util.Avro;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;

/**
 * HBase bulk import example
 * <p>
 * Parses Transactions from CSV files and outputs
 * <ImmutableBytesWritable, KeyValue>.
 * <p>
 * The ImmutableBytesWritable key is used by the TotalOrderPartitioner to map it
 * into the correct HBase table region.
 * <p>
 * The KeyValue value holds the HBase mutation information (column family,
 * column, and value)
 *
 * @author abykovsky
 * @since 1/29/15
 */
public class TransactionsDataLoadKVMapper extends
        Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

    private final static int NUM_FIELDS = 12;

    CSVParser csvParser = new CSVParser();

    String tableName = "";

    ImmutableBytesWritable hKey = new ImmutableBytesWritable();
    KeyValue kv;

    SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yy");

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

        if (value.find("ORDER_ID") > -1) {
            // Skip header
            return;
        }

        String[] fields;

        try {
            fields = csvParser.parseLine(value.toString());
        } catch (Exception ex) {
            context.getCounter("TransactionsDataLoadKVMapper", "PARSE_ERRORS").increment(1);
            return;
        }

        if (fields.length != NUM_FIELDS) {
            context.getCounter("TransactionsDataLoadKVMapper", "INVALID_FIELD_LEN").increment(1);
            return;
        }

        String orderDttm = fields[7];
        Date orderDate;
        try {
            orderDate = dateFormat.parse(orderDttm);
        } catch (ParseException e) {
            context.getCounter("TransactionsDataLoadKVMapper", "INVALID_ORDER_DATE").increment(1);
            return;
        }

        Transaction transaction;
        try {
            transaction = createTransaction(fields);
        } catch (NumberFormatException e) {
            context.getCounter("TransactionsDataLoadKVMapper", "INVALID_NUMBER").increment(1);
            return;
        }

        // Key: e.g. ":"
        hKey.set(String.format("%s:%s", orderDate.getTime(), transaction.getId()).getBytes());

        if (!transaction.getItems().isEmpty()) {
            kv = new KeyValue(hKey.get(), TransactionsDaoImpl.INFO_FAM, TransactionsDaoImpl.ITEMS_COLUMN,
                    Avro.transactionToBytes(transaction));
            context.write(hKey, kv);
        }

        context.getCounter("TransactionsDataLoadKVMapper", "NUM_MSGS").increment(1);
    }

    private Transaction createTransaction(String fields[]) {
        Transaction transaction = new Transaction();
        transaction.setId(fields[0]);
        transaction.setItems(createItems(fields[2]));
        transaction.setQuantities(createQuantities(fields[5]));
        return transaction;
    }

    private List<Integer> createQuantities(String field) {
        List<Integer> result = new ArrayList<>();
        for (String item : field.split("\\|")) {
            if(!item.trim().isEmpty()) result.add(Integer.parseInt(item));
        }
        return result;
    }

    private List<CharSequence> createItems(String field) {
        List<CharSequence> result = new ArrayList<>();
        for (String item : field.split("\\|")) {
            if(!item.trim().isEmpty()) result.add(item);
        }
        return result;
    }
}
