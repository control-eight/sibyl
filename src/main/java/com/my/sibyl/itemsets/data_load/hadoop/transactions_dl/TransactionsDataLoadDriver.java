package com.my.sibyl.itemsets.data_load.hadoop.transactions_dl;

import com.my.sibyl.itemsets.hbase.dao.TransactionsDaoImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

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
        Configuration conf = HBaseConfiguration.create();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.set("hbase.table.name", TransactionsDaoImpl.TABLE_NAME_STRING);

        // Load hbase-site.xml
        HBaseConfiguration.addHbaseResources(conf);

        Job job = new Job(conf, "HBase Bulk Import Transactions");
        //
        job.setJarByClass(TransactionsDataLoadKVMapper.class);

        job.setMapperClass(TransactionsDataLoadKVMapper.class);
        //out put key's class for IdentityTableReducer
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        //out put value's class for IdentityTableReducer
        job.setMapOutputValueClass(Put.class);

        //text which represents one line of csv file
        job.setInputFormatClass(TextInputFormat.class);

        //hbase standard reducer which accepts Put, Delete any Mutation operation
        TableMapReduceUtil.initTableReducerJob(TransactionsDaoImpl.TABLE_NAME_STRING, IdentityTableReducer.class, job);

        //read file from file system, in our case transactions csv
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.waitForCompletion(true);
    }
}