package com.my.sibyl.itemsets.data_load.hadoop.transactions_dl.group;

import com.my.sibyl.itemsets.ConfigurationHolder;
import com.my.sibyl.itemsets.data_load.hadoop.transactions_dl.group.dto.TransactionDto;
import com.my.sibyl.itemsets.hbase.dao.InstancesDaoImpl;
import com.my.sibyl.itemsets.hbase.dao.TransactionsDaoImpl;
import com.my.sibyl.itemsets.model.Instance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Iterator;
import java.util.Map;

/**
 * HBase bulk import example<br>
 * Data preparation MapReduce job driver
 * <ol>
 * <li>args[0]: HDFS input path
 * <li>args[1]: HDFS output path
 * <li>args[2]: HBase table name
 * </ol>
 *
 * @author abykovsky
 * @since 1/29/15
 */
public class TransactionsDataLoadDriver {

    public static void main(String[] args) throws Exception {
        org.apache.commons.configuration.Configuration envConfiguration = ConfigurationHolder.getConfiguration();
        Configuration conf = HBaseConfiguration.create();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Instance instance;
        try(HConnection connection = HConnectionManager.createConnection(conf)) {
            instance = new InstancesDaoImpl(connection).get(args[1]);
        }

        if(instance == null) throw new RuntimeException("Instance \"" + args[1] + "\" isn't found!");

        // Load hbase-site.xml
        HBaseConfiguration.addHbaseResources(conf);

        for (Iterator<String> keyIter = envConfiguration.getKeys(); keyIter.hasNext();) {
            String key = keyIter.next();
            conf.set(key, envConfiguration.getString(key));
        }

        Job job = new Job(conf, "HBase Bulk Import Transactions");
        //
        job.setJarByClass(TransactionsDataLoadMapper.class);

        job.setMapperClass(TransactionsDataLoadMapper.class);
        //out put key's class for TransactionsDataLoadReducer
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        //out put value's class for TransactionsDataLoadReducer
        job.setMapOutputValueClass(TransactionDto.class);

        //text which represents one line of csv file
        job.setInputFormatClass(TextInputFormat.class);

        //hbase standard reducer which accepts Put, Delete any Mutation operation
        TableMapReduceUtil.initTableReducerJob(TransactionsDaoImpl.getTableName(instance.getName()),
                TransactionsDataLoadReducer.class, job);

        //read file from file system, in our case transactions csv
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.waitForCompletion(true);
    }
}