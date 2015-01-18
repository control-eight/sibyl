package com.my.sibyl.itemsets.kiji.tests;

import com.my.sibyl.itemsets.ItemSetsFilter;
import com.my.sibyl.itemsets.kiji.KijiFrequentItemSetsGenerator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author abykovsky
 * @since 12/20/14
 */
public class ImportItemSets extends Configured implements Tool {

    public static final int MAX_BASKET_ITEMS_COUNT = 7;
    /** URI of Kiji instance to use (need to support Cassandra and HBase Kiji). */
    @Flag(
            name="kiji",
            usage="Specify the Kiji instance containing the 'itemsets' table."
    )
    private String mKijiUri = "kiji://.env/default";

    @Flag(
            name="importFile",
            usage="File to import. With user transactions"
    )
    private String importFile;

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
        KijiTableWriter writer = null;
        AtomicKijiPutter putter = null;
        KijiTableReader reader = null;
        try {
            // Connect to Kiji and open the table.
            kiji = Kiji.Factory.open(KijiURI.newBuilder(mKijiUri).build(), getConf());
            table = kiji.openTable(TABLE_NAME);
            writer = table.openTableWriter();
            putter = table.getWriterFactory().openAtomicPutter();
            reader = table.openTableReader();

            KijiFrequentItemSetsGenerator frequentItemSetsGenerator = new KijiFrequentItemSetsGenerator(table, writer, putter, null);
            ItemSetsFilter itemSetsFilter = new ItemSetsFilter();

            try(Reader in = new FileReader(importFile)) {
                Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader().parse(in);
                System.out.println("Start load data from \"" + importFile + "\"");
                int i = 0;
                int all = 0;
                for (CSVRecord record : records) {

                    List<Double> actualPriceList = new ArrayList<>();
                    for (String s : record.get("PROD_ACTL_PRCS").split("\\|")) {
                        s = s.trim();
                        if (s.length() > 0) {
                            actualPriceList.add(Double.parseDouble(s));
                        }
                    }

                    List<Long> transactionItems = new ArrayList<>();
                    for (String s : record.get("PROD_PRCHSD").split("\\|")) {
                        s = s.trim();
                        if (s.length() > 0) {
                            transactionItems.add(Long.parseLong(s));
                        }
                    }

                    itemSetsFilter.filterFreePrice(transactionItems, actualPriceList);

                    all++;
                    if (transactionItems.size() > MAX_BASKET_ITEMS_COUNT) {
                        continue;
                    }

                    frequentItemSetsGenerator.add(transactionItems);
                    if (++i % 10000 == 0) System.out.println("============= 10000 rows were processed =============");
                }
                System.out.println("End load data. " + i + " rows were processed from " + all);
            }
        } finally {
            // Safely free up resources by closing in reverse order.
            ResourceUtils.releaseOrLog(table);
            ResourceUtils.releaseOrLog(kiji);
            ResourceUtils.closeOrLog(writer);
            ResourceUtils.closeOrLog(putter);
            ResourceUtils.closeOrLog(reader);
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
        System.exit(ToolRunner.run(new ImportItemSets(), args));
    }
}
