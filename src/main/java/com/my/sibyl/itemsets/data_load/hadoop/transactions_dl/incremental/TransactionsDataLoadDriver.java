package com.my.sibyl.itemsets.data_load.hadoop.transactions_dl.incremental;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.set("hbase.table.name", args[2]);

        // Load hbase-site.xml
        HBaseConfiguration.addHbaseResources(conf);

        Job job = new Job(conf, "HBase Bulk Import Transactions");
        job.setJarByClass(TransactionsDataLoadKVMapper.class);

        job.setMapperClass(TransactionsDataLoadKVMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        job.setInputFormatClass(TextInputFormat.class);

        HTable hTable = new HTable(conf, args[2]);

        // Auto configure partitioner and reducer
        HFileOutputFormat2.configureIncrementalLoad(job, hTable);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
