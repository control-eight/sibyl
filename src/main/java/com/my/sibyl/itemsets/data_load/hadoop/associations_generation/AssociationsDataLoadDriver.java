package com.my.sibyl.itemsets.data_load.hadoop.associations_generation;

import com.my.sibyl.itemsets.AssociationServiceImpl;
import com.my.sibyl.itemsets.InstancesService;
import com.my.sibyl.itemsets.hbase.dao.InstancesDaoImpl;
import com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl;
import com.my.sibyl.itemsets.hbase.dao.TransactionsDaoImpl;
import com.my.sibyl.itemsets.model.Association;
import com.my.sibyl.itemsets.model.Instance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author abykovsky
 * @since 2/1/15
 */
public class AssociationsDataLoadDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Instance instance;
        try(HConnection connection = HConnectionManager.createConnection(conf)) {
            instance = new InstancesDaoImpl(connection).get(args[0]);
        }

        // Load hbase-site.xml
        HBaseConfiguration.addHbaseResources(conf);

        Job job = new Job(conf, "HBase Bulk Import/Generate Assocations");
        job.setJarByClass(AssociationsMapper.class);

        Scan scan = TransactionsDaoImpl.makeTransactionsScan(instance.getStartLoadDate(), instance.getEndLoadDate());

        TableMapReduceUtil.initTableMapperJob(TransactionsDaoImpl.TABLE_NAME, // input table
                scan, // Scan instance
                AssociationsMapper.class, // mapper class
                ImmutableBytesWritable.class, // mapper output key
                Association.class, // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(ItemSetsDaoImpl.getTableName(instance.getName()),
                AssociationsReducer.class, job);

        job.waitForCompletion(true);

        try(HConnection connection = HConnectionManager.createConnection(conf)) {
            new ItemSetsDaoImpl(connection).incrementItemSetCount(InstancesService.DEFAULT,
                    AssociationServiceImpl.TRANSACTIONS_COUNT_ROW_KEY,
                    job.getCounters().findCounter(AssociationsMapper.Counters.ROWS).getValue());
        }

    }
}
