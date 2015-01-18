package com.my.sibyl.itemsets.kiji.tests;

import com.my.sibyl.itemsets.kiji.KijiSlidingWindow;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/11/15
 */
public class SlidingWindowSimultation extends Configured implements Tool {

    /** URI of Kiji instance to use (need to support Cassandra and HBase Kiji). */
    @Flag(
            name="kiji",
            usage="Specify the Kiji instance containing the 'sliding_window' table."
    )
    private String mKijiUri = "kiji://.env/default";

    /** Name of the table to read for ids entries. */
    public static final String TABLE_NAME = "sliding_window";

    /**
     * Run the entry addition system. Asks the user for values for all fields
     * and then fills them in.
     *
     * @param args Command line arguments.
     * @return Exit status code for the application; 0 indicates success.
     * @throws java.io.IOException If an error contacting Kiji occurs.
     * @throws InterruptedException If the process is interrupted while performing I/O.
     */
    @Override
    public int run(String[] args) throws Exception {
        // Parse command-line arguments, populating mKijiUri.
        List<String> nonFlagArgs = FlagParser.init(this, args);
        if (null == nonFlagArgs) {
            // There was a problem parsing the flags.
            return 1;
        }

        Kiji kiji = null;
        KijiTable table = null;
        KijiTableWriter writer = null;
        KijiTableReader reader = null;

        try {
            // Load HBase configuration before connecting to Kiji.
            setConf(HBaseConfiguration.addHbaseResources(getConf()));

            // Connect to Kiji and open the table.
            kiji = Kiji.Factory.open(KijiURI.newBuilder(mKijiUri).build(), getConf());
            table = kiji.openTable(TABLE_NAME);
            writer = table.openTableWriter();
            reader = table.openTableReader();

            KijiSlidingWindow slidingWindow = new KijiSlidingWindow(5 * 1000 * 1000_000l, table, writer, reader,
                    null, null, null);

            while (true) {
                long txId = (long) (Math.random() * 1000_000_000);
                List<Long> transactionItems = new ArrayList<>();
                for(int i = 0; i < Math.random() * 7; i++) {
                    transactionItems.add((long) (Math.random() * 1000_000_000));
                }
                SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
                System.out.println(format.format(new Date()) + " " + "Add transaction");
                slidingWindow.addTransaction(txId, transactionItems);
                Thread.sleep(900);
            }
        } catch (KijiTableNotFoundException e) {
            System.out.println("Could not find Kiji table: " + TABLE_NAME);
            return 1;
        } finally {
            // Safely free up resources by closing in reverse order.
            ResourceUtils.closeOrLog(reader);
            ResourceUtils.closeOrLog(writer);
            ResourceUtils.releaseOrLog(table);
            ResourceUtils.releaseOrLog(kiji);
        }
    }

    /**
     * Program entry point. Terminates the application without returning.
     *
     * @param args The arguments from the command line. May start with Hadoop "-D" options.
     * @throws Exception If the application encounters an exception.
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SlidingWindowSimultation(), args));
    }
}
