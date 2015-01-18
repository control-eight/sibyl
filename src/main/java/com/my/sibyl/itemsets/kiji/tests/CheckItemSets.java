package com.my.sibyl.itemsets.kiji.tests;

import com.my.sibyl.itemsets.kiji.ItemSetsFields;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

import java.util.List;

/**
 * @author abykovsky
 * @since 1/11/15
 */
public class CheckItemSets extends Configured implements Tool {

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
        Kiji kiji = null;
        KijiTable table = null;
        KijiTableReader reader = null;
        KijiRowScanner scanner = null;
        try {
            // Load HBase configuration before connecting to Kiji.
            setConf(HBaseConfiguration.addHbaseResources(getConf()));

            // Connect to Kiji and open the table.
            kiji = Kiji.Factory.open(KijiURI.newBuilder(mKijiUri).build(), getConf());
            table = kiji.openTable(TABLE_NAME);
            reader = table.openTableReader();

            final KijiDataRequest dataRequest = KijiDataRequest.builder()
                    .addColumns(KijiDataRequestBuilder.ColumnsDef.create().add(ItemSetsFields.INFO_FAMILY.getName(),
                            ItemSetsFields.COUNT.getName())).build();

            final KijiTableReader.KijiScannerOptions scanOptions = new KijiTableReader.KijiScannerOptions()
                    .setStartRow(table.getEntityId("1"))
                            //.setStartRow(table.getEntityId(8754321))
                    .setStopRow(table.getEntityId("z"));

            scanner = reader.getScanner(dataRequest, scanOptions);
            int i = 0;
            for (KijiRowData row : scanner) {
                Long count = row.getMostRecentValue(ItemSetsFields.INFO_FAMILY.getName(),
                        ItemSetsFields.COUNT.getName());
                if(count > 0) {
                    System.out.println(row.getEntityId().toShellString() + " "
                     + count);
                }
                if(++i % 5000 == 0) System.out.println("===5000===");
            }
        } catch (KijiTableNotFoundException e) {
            System.out.println("Could not find Kiji table: " + TABLE_NAME);
            return 1;
        } finally {
            // Safely free up resources by closing in reverse order.
            ResourceUtils.closeOrLog(scanner);
            ResourceUtils.closeOrLog(reader);
            ResourceUtils.releaseOrLog(table);
            ResourceUtils.releaseOrLog(kiji);
        }

        return 0;
    }

    /**
     * Program entry point. Terminates the application without returning.
     *
     * @param args The arguments from the command line. May start with Hadoop "-D" options.
     * @throws Exception If the application encounters an exception.
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CheckItemSets(), args));
    }
}
