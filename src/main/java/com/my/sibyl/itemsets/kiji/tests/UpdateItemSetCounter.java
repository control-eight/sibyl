package com.my.sibyl.itemsets.kiji.tests;

import com.my.sibyl.itemsets.kiji.ItemSetsFields;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

import java.util.List;

/**
 * @author abykovsky
 * @since 12/20/14
 */
public class UpdateItemSetCounter extends Configured implements Tool {

    /** URI of Kiji instance to use (need to support Cassandra and HBase Kiji). */
    @Flag(
            name="kiji",
            usage="Specify the Kiji instance containing the 'itemsets' table."
    )
    private String mKijiUri = "kiji://.env/default";

    /** Name of the table to read for itemsets entries. */
    public static final String TABLE_NAME = "itemsets";

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
        final ConsolePrompt console = new ConsolePrompt();

        // Interactively prompt the user for the record fields from the console.

        //Multiple productId separated by ","
        String itemSet = console.readLine("ItemSet: ");
        final Integer count = Integer.parseInt(console.readLine("Count: "));

        itemSet = format(itemSet);

        Kiji kiji = null;
        KijiTable table = null;
        KijiTableWriter writer = null;
        try {
            // Load HBase configuration before connecting to Kiji.
            setConf(HBaseConfiguration.addHbaseResources(getConf()));

            // Connect to Kiji and open the table.
            kiji = Kiji.Factory.open(KijiURI.newBuilder(mKijiUri).build(), getConf());
            table = kiji.openTable(TABLE_NAME);
            writer = table.openTableWriter();

            // Create a row ID with itemSet.
            final EntityId itemSetId = table.getEntityId(itemSet);

            // Update increment
            KijiCell<Long> result = writer.increment(itemSetId, ItemSetsFields.INFO_FAMILY.getName(),
                    ItemSetsFields.COUNT.getName(), count);
            System.out.println(result + " " + result.getData());
        } catch (KijiTableNotFoundException e) {
            System.out.println("Could not find Kiji table: " + TABLE_NAME);
            return 1;
        } finally {
            // Safely free up resources by closing in reverse order.
            ResourceUtils.closeOrLog(writer);
            ResourceUtils.releaseOrLog(table);
            ResourceUtils.releaseOrLog(kiji);
            ResourceUtils.closeOrLog(console);
        }

        return 0;
    }

    private String format(String itemSet) {
        StringBuilder result = new StringBuilder();
        for (String s : itemSet.split(",")) {
            result.append(s.trim()).append("-");
        }
        return result.substring(0, result.length() - 1);
    }

    /**
     * Program entry point. Terminates the application without returning.
     *
     * @param args The arguments from the command line. May start with Hadoop "-D" options.
     * @throws Exception If the application encounters an exception.
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new UpdateItemSetCounter(), args));
    }
}
