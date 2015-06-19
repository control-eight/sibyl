package com.my.sibyl.itemsets.data_load.hadoop.transactions_dl;

import au.com.bytecode.opencsv.CSVParser;
import com.my.sibyl.itemsets.model.Transaction;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * HBase bulk import example
 * <p>
 * Parses Facebook and Twitter messages from CSV files and outputs
 * <ImmutableBytesWritable, Put>.
 *
 * @author abykovsky
 * @since 1/29/15
 */
public class TransactionsDataLoadKVMapper extends TransactionsDataLoadKVMapperAbstract {

    private final static int NUM_FIELDS = 12;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yy");

    private final CSVParser csvParser = new CSVParser();

    @Override
    protected boolean isHeader(Text value) {
        return value.find("ORDER_ID") > -1;
    }

    @Override
    protected int getNumFields() {
        return NUM_FIELDS;
    }

    @Override
    protected String getClassName() {
        return "TransactionsDataLoadKVMapper";
    }

    @Override
    protected String[] parseLine(Text value) throws IOException {
        return csvParser.parseLine(value.toString());
    }

    protected Transaction createTransaction(String fields[], Context context) {
        String orderDttm = fields[7];
        Date orderDate;
        try {
            orderDate = getDateFormat().parse(orderDttm);
        } catch (ParseException e) {
            context.getCounter(getClassName(), "INVALID_ORDER_DATE").increment(1);
            return null;
        }

        Transaction transaction = new Transaction();
        transaction.setId(fields[0]);
        transaction.setItems(createItems(fields[2]));
        transaction.setQuantities(createQuantities(fields[5]));
        transaction.setCreateTimestamp(orderDate.getTime());
        return transaction;
    }

    private SimpleDateFormat getDateFormat() {
        return dateFormat;
    }

    private List<Integer> createQuantities(String field) {
        List<Integer> result = new ArrayList<>();
        for (String item : field.split("\\|")) {
            if(!item.trim().isEmpty()) result.add(Integer.parseInt(item));
        }
        return result;
    }

    private List<String> createItems(String field) {
        List<String> result = new ArrayList<>();
        for (String item : field.split("\\|")) {
            if(!item.trim().isEmpty()) result.add(item);
        }
        return result;
    }
}
