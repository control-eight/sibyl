package com.my.sibyl.itemsets.data_load.hadoop.associations_generation;

import com.my.sibyl.itemsets.model.Association;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl.*;

/**
 * @author abykovsky
 * @since 2/1/15
 */
public class AssociationsReducer extends TableReducer<ImmutableBytesWritable, Association, ImmutableBytesWritable> {

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Association> values, Context context)
            throws IOException, InterruptedException {
        Put put = new Put(key.get());
        long itemSetCount = 0;

        Map<String, AtomicLong> associationsMap = new HashMap<>();
        for (Association value : values) {
            itemSetCount += value.getItemSetCount();
            if("".equals(value.getAssociationId())) continue;
            associationsMap.putIfAbsent(value.getAssociationId(), new AtomicLong(0l));
            associationsMap.get(value.getAssociationId()).addAndGet(value.getAssociationCount());
        }
        for (Map.Entry<String, AtomicLong> entry : associationsMap.entrySet()) {
            put.add(ASSOCIATION_FAM, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().get()));
        }
        put.add(COUNT_FAM, COUNT_COL, Bytes.toBytes(itemSetCount));
        context.write(key, put);
    }
}
