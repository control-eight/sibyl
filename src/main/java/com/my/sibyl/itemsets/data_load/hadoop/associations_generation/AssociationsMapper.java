package com.my.sibyl.itemsets.data_load.hadoop.associations_generation;

import com.my.sibyl.itemsets.ConfigurationHolder;
import com.my.sibyl.itemsets.ItemSetAndAssociation;
import com.my.sibyl.itemsets.hbase.dao.TransactionsDaoImpl;
import com.my.sibyl.itemsets.model.Association;
import com.my.sibyl.itemsets.model.Transaction;
import com.my.sibyl.itemsets.util.Avro;
import com.my.sibyl.itemsets.util.ItemSetsGenerator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * @author abykovsky
 * @since 2/1/15
 */
public class AssociationsMapper extends TableMapper<ImmutableBytesWritable, Association> {

    public static enum Counters {
        ROWS,
        ITEM_SET_WITH_ASSOCIATIONS,
        ASSOCIATIONS
    }

    private int maxItemSetLength = (int) ConfigurationHolder.getConfiguration()
            .getInt("maxItemSetLength");

    private ItemSetsGenerator itemSetsGenerator = new ItemSetsGenerator(maxItemSetLength);

    public void map(ImmutableBytesWritable row, Result value,
                    Context context) throws InterruptedException, IOException {
        Transaction transaction = Avro.bytesToTransaction(value
                .getValue(TransactionsDaoImpl.INFO_FAM, TransactionsDaoImpl.ITEMS_COLUMN));

        Collection<ItemSetAndAssociation<String>> itemSetAndAssociations = itemSetsGenerator
                .generateItemSetsAndAssociations(transaction.getItems(), 1);

        context.getCounter(Counters.ITEM_SET_WITH_ASSOCIATIONS).increment(itemSetAndAssociations.size());
        for (ItemSetAndAssociation<String> itemSetAndAssociation : itemSetAndAssociations) {
            boolean itemSetCountSet = false;
            for (Map.Entry<String, Long> association : itemSetAndAssociation.getAssociationMap().entrySet()) {
                //set only for first, because we don't need to duplicate
                long itemSetCount = 0;
                if(!itemSetCountSet) {
                    itemSetCount = itemSetAndAssociation.getCount();
                    itemSetCountSet = true;
                }

                context.write(new ImmutableBytesWritable(Bytes.toBytes(itemSetAndAssociation.getItemSet())),
                        new Association(itemSetCount, association.getKey(), association.getValue()));
                context.getCounter(Counters.ASSOCIATIONS).increment(1);
            }
            if(itemSetAndAssociation.getAssociationMap().isEmpty()) {
                context.write(new ImmutableBytesWritable(Bytes.toBytes(itemSetAndAssociation.getItemSet())),
                        new Association(itemSetAndAssociation.getCount(), "", -1));
            }
        }

        context.getCounter(Counters.ROWS).increment(1);
    }
}
