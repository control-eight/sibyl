package com.my.sibyl.itemsets.kiji.tests;

import com.my.sibyl.itemsets.kiji.KijiCandidatesGenerator;
import com.my.sibyl.itemsets.kiji.KijiFrequentItemSetsGenerator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Add recommendation using KijiCandidatesGenerator
 * @author abykovsky
 * @since 12/25/14
 */
public class AddRecommendation2 extends Configured implements Tool {

    /** URI of Kiji instance to use (need to support Cassandra and HBase Kiji). */
    @SuppressWarnings("FieldCanBeLocal")
    @Flag(
            name="kiji",
            usage="Specify the Kiji instance containing the 'recommendations' table."
    )
    private String mKijiUri = "kiji://.env/default";

    public static final String ITEM_SETS_TABLE_NAME = "itemsets";

    /** Name of the table to read for itemsets entries. */
    public static final String TABLE_NAME = "recommendations";

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
        final String itemSet = console.readLine("ItemSet: ");
        final Integer count = Integer.parseInt(console.readLine("Count: "));
        final Integer transactionCount = Integer.parseInt(console.readLine("Transaction Count: "));

        Kiji kiji = null;
        KijiTable table = null;
        KijiTableReader reader = null;
        AtomicKijiPutter putter = null;

        KijiTable itemSetsTable = null;
        KijiTableReader itemSetsReader = null;
        try {
            // Load HBase configuration before connecting to Kiji.
            setConf(HBaseConfiguration.addHbaseResources(getConf()));

            // Connect to Kiji and open the table.
            kiji = Kiji.Factory.open(KijiURI.newBuilder(mKijiUri).build(), getConf());
            table = kiji.openTable(TABLE_NAME);
            reader = table.openTableReader();
            putter = table.getWriterFactory().openAtomicPutter();

            itemSetsTable = kiji.openTable(ITEM_SETS_TABLE_NAME);
            itemSetsReader = itemSetsTable.openTableReader();

            KijiFrequentItemSetsGenerator frequentItemSetsGenerator = new KijiFrequentItemSetsGenerator(table, null, null,
                    itemSetsReader);
            KijiCandidatesGenerator kijiCandidatesGenerator = new KijiCandidatesGenerator(table, putter, reader,
                    frequentItemSetsGenerator);

            Map<Set<Long>, Pair<Integer, Integer>> itemSetsDifference = new HashMap<>();
            Set<Long> parse = parse(itemSet);
            itemSetsDifference.put(parse, new ImmutablePair<>(count, count));
            for (Long id : parse) {
                itemSetsDifference.put(set(id), new ImmutablePair<>(10, 40));
            }
            System.out.println("Difference map: " + itemSetsDifference);

            kijiCandidatesGenerator.process(itemSetsDifference, transactionCount);

        } catch (KijiTableNotFoundException e) {
            System.out.println("Could not find Kiji table: " + TABLE_NAME);
            return 1;
        } finally {
            // Safely free up resources by closing in reverse order.
            ResourceUtils.closeOrLog(reader);
            ResourceUtils.closeOrLog(putter);
            ResourceUtils.releaseOrLog(table);

            ResourceUtils.closeOrLog(itemSetsReader);
            ResourceUtils.releaseOrLog(itemSetsTable);

            ResourceUtils.releaseOrLog(kiji);
        }

        return 0;
    }

    private Set<Long> set(Long item) {
        Set<Long> result = new HashSet<>();
        result.add(item);
        return result;
    }

    private Set<Long> parse(String itemSet) {
        Set<Long> result = new HashSet<>();
        for (String item : itemSet.split(",")) {
            result.add(Long.parseLong(item.trim()));
        }
        return result;
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
        System.exit(ToolRunner.run(new AddRecommendation2(), args));
    }
}
