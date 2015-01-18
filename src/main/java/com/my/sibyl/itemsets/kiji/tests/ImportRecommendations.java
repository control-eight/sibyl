package com.my.sibyl.itemsets.kiji.tests;

import com.my.sibyl.itemsets.ItemSetsFilter;
import com.my.sibyl.itemsets.kiji.KijiCandidatesGenerator;
import com.my.sibyl.itemsets.kiji.KijiFrequentItemSetsGenerator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;
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
import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 12/27/14
 */
public class ImportRecommendations extends Configured implements Tool {

    public static final int MAX_BASKET_ITEMS_COUNT = 7;
    public static final int INT = 1000;
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
    public static final String ITEM_SETS_TABLE_NAME = "itemsets";

    /** Name of the table to read for itemsets entries. */
    public static final String RECOMMENDATIONS_TABLE_NAME = "recommendations";

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

        KijiTable itemSetsTable = null;
        KijiTableWriter itemSetsWriter = null;
        AtomicKijiPutter itemSetsPutter = null;
        KijiTableReader itemSetsReader = null;

        KijiTable recommendationsTable = null;
        AtomicKijiPutter recommendationsPutter = null;
        KijiTableReader recommendationsReader = null;
        try {
            // Connect to Kiji and open the itemSetsTable.
            kiji = Kiji.Factory.open(KijiURI.newBuilder(mKijiUri).build(), getConf());

            itemSetsTable = kiji.openTable(ITEM_SETS_TABLE_NAME);
            itemSetsWriter = itemSetsTable.openTableWriter();
            itemSetsPutter = itemSetsTable.getWriterFactory().openAtomicPutter();
            itemSetsReader = itemSetsTable.openTableReader();

            recommendationsTable = kiji.openTable(RECOMMENDATIONS_TABLE_NAME);
            recommendationsPutter = recommendationsTable.getWriterFactory().openAtomicPutter();
            recommendationsReader = recommendationsTable.openTableReader();

            KijiFrequentItemSetsGenerator frequentItemSetsGenerator = new KijiFrequentItemSetsGenerator(itemSetsTable,
                    itemSetsWriter, itemSetsPutter, itemSetsReader);
            ItemSetsFilter itemSetsFilter = new ItemSetsFilter();
            KijiCandidatesGenerator candidatesGenerator = new KijiCandidatesGenerator(recommendationsTable,
                    recommendationsPutter, recommendationsReader, frequentItemSetsGenerator);

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

                    Map<Set<Long>, Pair<Integer, Integer>> difference = frequentItemSetsGenerator.add(transactionItems);
                    candidatesGenerator.process(difference, i);

                    if (++i % INT == 0) System.out.println("============= " + INT + " rows were processed =============");
                }
                System.out.println("End load data. " + i + " rows were processed from " + all);
            }
        } finally {
            // Safely free up resources by closing in reverse order.
            ResourceUtils.closeOrLog(recommendationsReader);
            ResourceUtils.closeOrLog(recommendationsPutter);
            ResourceUtils.releaseOrLog(recommendationsTable);

            ResourceUtils.closeOrLog(itemSetsReader);
            ResourceUtils.closeOrLog(itemSetsPutter);
            ResourceUtils.closeOrLog(itemSetsWriter);
            ResourceUtils.releaseOrLog(itemSetsTable);

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
        System.exit(ToolRunner.run(new ImportRecommendations(), args));
    }
}
