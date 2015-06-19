package com.my.sibyl.itemsets.data_load.hadoop.transactions_dl.group;

import com.my.sibyl.itemsets.data_load.hadoop.transactions_dl.group.dto.TransactionDto;
import com.my.sibyl.itemsets.hbase.dao.TransactionsDaoImpl;
import com.my.sibyl.itemsets.model.Transaction;
import com.my.sibyl.itemsets.util.Avro;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author abykovsky
 * @since 6/18/15
 */
public class TransactionsDataLoadReducer extends TableReducer<ImmutableBytesWritable, TransactionDto, ImmutableBytesWritable> {

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<TransactionDto> values, Context context)
            throws IOException, InterruptedException {
        Transaction resultTransaction = new Transaction();
        resultTransaction.setItems(new ArrayList<>());
        resultTransaction.setQuantities(new ArrayList<>());

        for (TransactionDto value : values) {
            resultTransaction.setId(value.getId());

            int indexOf = resultTransaction.getItems().indexOf(value.getItem());
            if(indexOf != -1) {
                resultTransaction.getQuantities().set(indexOf,
                        resultTransaction.getQuantities().get(indexOf) + value.getQuantity());
            } else {
                resultTransaction.getItems().add(value.getItem());
                resultTransaction.getQuantities().add(value.getQuantity());
            }
            resultTransaction.setCreateTimestamp(value.getCreateTimestamp());
        }

        Put put = new Put(key.get());
        put.add(TransactionsDaoImpl.INFO_FAM, TransactionsDaoImpl.ITEMS_COLUMN, Avro.transactionToBytes(resultTransaction));

        context.write(key, put);
    }
}