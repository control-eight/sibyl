package com.my.sibyl.itemsets.kiji.tests;

import com.my.sibyl.itemsets.ItemSetsFilter;
import com.my.sibyl.itemsets.kiji.KijiCandidatesGenerator;
import com.my.sibyl.itemsets.kiji.KijiFrequentItemSetsGenerator;
import com.my.sibyl.itemsets.kiji.KijiSlidingWindow;
import com.my.sibyl.itemsets.kiji.Transaction;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author abykovsky
 * @since 1/11/15
 */
public class RecommendationsWithSlidingWindowSimulation extends Configured implements Tool {

    public static final int MAX_BASKET_ITEMS_COUNT = 7;
    public static final int INT = 10000;
    /**
     * URI of Kiji instance to use (need to support Cassandra and HBase Kiji).
     */
    @Flag(
            name = "kiji",
            usage = "Specify the Kiji instance containing the 'itemsets' table."
    )
    private String mKijiUri = "kiji://.env/default";

    @Flag(
            name = "importFile",
            usage = "File to import. With user transactions"
    )
    private String importFile;

    public static final String ITEM_SETS_TABLE_NAME = "itemsets";
    public static final String RECOMMENDATIONS_TABLE_NAME = "recommendations";
    public static final String SLIDING_WINDOW_TABLE_NAME = "sliding_window";

    /**
     * Run the entry addition system. Asks the user for values for all fields
     * and then fills them in.
     *
     * @param args Command line arguments.
     * @return Exit status code for the application; 0 indicates success.
     * @throws java.io.IOException  If an error contacting Kiji occurs.
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

        KijiTable slidingWindowTable = null;
        KijiTableWriter slidingWindowWriter = null;
        KijiTableReader slidingWindowReader = null;
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

            slidingWindowTable = kiji.openTable(SLIDING_WINDOW_TABLE_NAME);
            slidingWindowWriter = slidingWindowTable.openTableWriter();
            slidingWindowReader = slidingWindowTable.openTableReader();

            final AtomicInteger txCount = new AtomicInteger(0);
            ItemSetsFilter itemSetsFilter = new ItemSetsFilter();

            KijiFrequentItemSetsGenerator frequentItemSetsGenerator = new KijiFrequentItemSetsGenerator(itemSetsTable,
                    itemSetsWriter, itemSetsPutter, itemSetsReader);

            KijiCandidatesGenerator candidatesGenerator = new KijiCandidatesGenerator(recommendationsTable,
                    recommendationsPutter, recommendationsReader, frequentItemSetsGenerator);

            KijiSlidingWindow slidingWindow = new KijiSlidingWindow(TimeUnit.SECONDS.toNanos(10),
                    slidingWindowTable, slidingWindowWriter, slidingWindowReader,
                    frequentItemSetsGenerator, candidatesGenerator, txCount);

            //Thread.sleep(20000_000);

            try (Reader in = new FileReader(importFile)) {
                Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader().parse(in);
                System.out.println("Start load data from \"" + importFile + "\"");
                for (CSVRecord record : records) {
                    List<Long> transactionItems = getTransactionList(itemSetsFilter, record);
                    if (transactionItems.isEmpty() || transactionItems.size() > MAX_BASKET_ITEMS_COUNT) {
                        continue;
                    }
                    printTxCount(txCount);

                    Map<Set<Long>, Pair<Integer, Integer>> difference = frequentItemSetsGenerator.add(transactionItems);
                    candidatesGenerator.process(difference, txCount.get());
                    slidingWindow.addTransaction(new Transaction(getOrderId(record), transactionItems));
                    Thread.sleep(10);
                }
                System.out.println("End load data. " + txCount.get() + " rows were processed");
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

            ResourceUtils.closeOrLog(slidingWindowReader);
            ResourceUtils.closeOrLog(slidingWindowWriter);
            ResourceUtils.releaseOrLog(slidingWindowTable);

            ResourceUtils.releaseOrLog(kiji);
        }

        return 0;
    }

    private long getOrderId(CSVRecord record) {
        String string = record.get("ORDER_ID");
        string = string.replaceAll("[^\\d]*", "");
        if(string.isEmpty()) return (long) (Math.random() * 1000_000_000);
        return Long.parseLong(string);
    }

    private void printTxCount(AtomicInteger txCount) {
        if (txCount.incrementAndGet() % INT == 0)
            System.out.println("============= " + INT + " rows were processed =============");
    }

    private List<Long> getTransactionList(ItemSetsFilter itemSetsFilter, CSVRecord record) {
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
        return transactionItems;
    }

    /**
     * Program entry point. Terminates the application without returning.
     *
     * @param args The arguments from the command line. May start with Hadoop "-D" options.
     * @throws Exception If the application encounters an exception.
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new RecommendationsWithSlidingWindowSimulation(), args));
    }
}
