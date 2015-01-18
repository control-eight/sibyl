package com.my.sibyl.itemsets.kiji;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.util.Bytes;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRegion;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author abykovsky
 * @since 1/10/15
 */
public class KijiSlidingWindow {

    private final List<String> keys = new ArrayList<>();

    private final KijiTable table;
    private final KijiTableWriter writer;
    private final KijiTableReader reader;
    private final long size;
    private final KijiFrequentItemSetsGenerator frequentItemSetsGenerator;
    private final KijiCandidatesGenerator candidatesGenerator;
    private final AtomicInteger transactionCount;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public KijiSlidingWindow(final long timeSize, final KijiTable table, final KijiTableWriter writer,
                             final KijiTableReader reader,
                             final KijiFrequentItemSetsGenerator frequentItemSetsGenerator,
                             final KijiCandidatesGenerator candidatesGenerator,
                             final AtomicInteger transactionCount) throws IOException {
        this.size = timeSize;
        this.table = table;
        this.writer = writer;
        this.reader = reader;
        this.frequentItemSetsGenerator = frequentItemSetsGenerator;
        this.candidatesGenerator = candidatesGenerator;
        this.transactionCount = transactionCount;
        fillKeys(table);
        startCheckSizeAndCleanTask();
    }

    public void startCheckSizeAndCleanTask() throws IOException {
        scheduler.scheduleAtFixedRate(this::checkSizeAndClean, 0, 10, TimeUnit.SECONDS);
    }

    private void fillKeys(KijiTable table) throws IOException {
        for (KijiRegion kijiRegion : table.getRegions()) {
            String key = Bytes.toString(kijiRegion.getStartKey());
            if(!key.trim().isEmpty() && !keys.contains(key)) {
                keys.add(key);
            }
            key = Bytes.toString(kijiRegion.getEndKey());
            if(!key.trim().isEmpty() && !keys.contains(key)) {
                keys.add(key);
            }
        }
        System.out.println("Keys: " + keys);
    }

    public void addTransaction(Long transactionId, List<Long> transactionItems) throws IOException {
        this.addTransaction(new Transaction(transactionId, transactionItems));
    }

    public void addTransaction(Transaction transaction) throws IOException {
        //SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
        //System.out.println(format.format(new Date()) + " " + "Add transaction");

        final long timestamp = System.currentTimeMillis();
        writer.put(table.getEntityId(generateShardKey(keys) + System.nanoTime()), ItemSetsFields.INFO_FAMILY.getName(),
                ItemSetsFields.TRANSACTION.getName(), timestamp, transaction);
    }

    private String generateShardKey(List<String> keys) {
        return keys.get((int) (Math.random() * keys.size())) + "-";
    }

    public void checkSizeAndClean() {
        long start = System.currentTimeMillis();
        long end = System.nanoTime() - size;
        List<Long> intervals = Arrays.asList(end - TimeUnit.DAYS.toNanos(1), end);

        System.out.println(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + " "
                + "start checkSizeAndClean");
        try {
            checkAndRemoveTransactions(intervals);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + " "
                + "checkSizeAndClean time: " + (System.currentTimeMillis() - start) + "ms");
    }

    private void checkAndRemoveTransactions(List<Long> intervals) throws IOException {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
                .addColumns(KijiDataRequestBuilder.ColumnsDef.create().add(ItemSetsFields.INFO_FAMILY.getName(),
                        ItemSetsFields.TRANSACTION.getName())).build();

        int count = 0;
        //find excess transactions
        for (String prefix : keys) {
            final KijiTableReader.KijiScannerOptions scanOptions = new KijiTableReader.KijiScannerOptions()
                    .setStartRow(table.getEntityId(prefix + "-" + intervals.get(0)))
                    .setStopRow(table.getEntityId(prefix + "-" + intervals.get(1)));

            try(KijiRowScanner scanner = reader.getScanner(dataRequest, scanOptions)) {
                // Scan over the requested row range, in order:
                for (KijiRowData row : scanner) {
                    // Process the row:
                    //System.out.println(row);
                    count++;
                    writer.deleteRow(row.getEntityId());

                    Transaction transaction = row.getMostRecentValue(ItemSetsFields.INFO_FAMILY.getName(),
                            ItemSetsFields.TRANSACTION.getName());

                    Map<Set<Long>, Pair<Integer, Integer>> difference = frequentItemSetsGenerator
                            .remove(transaction.getItems());
                    candidatesGenerator.process(difference, transactionCount.get());
                }
            }
        }
        System.out.println(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + " "
                + "Transactions were removed from sliding window: " + count);
    }

    /*public void checkSizeAndClean() {
        long end = System.nanoTime() - size;
        List<Long> intervals = Arrays.asList(end - TimeUnit.DAYS.toNanos(1), end);
        try {
            List<Transaction> transactionList = checkAndRemoveTransactions(intervals);
            //remove transactions from itemSets and recommendations.
            for (Transaction transaction : transactionList) {
                //TODO aggregation could be done here
                Map<Set<Long>, Pair<Integer, Integer>> difference = frequentItemSetsGenerator.remove(transaction.getItems());
                candidatesGenerator.process(difference, transactionCount.get());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Transaction> checkAndRemoveTransactions(List<Long> intervals) throws IOException {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
                .addColumns(KijiDataRequestBuilder.ColumnsDef.create().add(ItemSetsFields.INFO_FAMILY.getName(),
                        ItemSetsFields.TRANSACTION.getName())).build();

        List<Pair<EntityId, Transaction>> result = new ArrayList<>();

        //find excess transactions
        for (String prefix : keys) {
            final KijiTableReader.KijiScannerOptions scanOptions = new KijiTableReader.KijiScannerOptions()
                    .setStartRow(table.getEntityId(prefix + "-" + intervals.get(0)))
                    .setStopRow(table.getEntityId(prefix + "-" + intervals.get(1)));

            try(KijiRowScanner scanner = reader.getScanner(dataRequest, scanOptions)) {
                // Scan over the requested row range, in order:
                for (KijiRowData row : scanner) {
                    // Process the row:
                    //System.out.println(row);
                    result.add(new ImmutablePair<>(row.getEntityId(),
                            row.getMostRecentValue(ItemSetsFields.INFO_FAMILY.getName(),
                            ItemSetsFields.TRANSACTION.getName())));
                }
            }
        }

        //remove excess transactions
        for (Pair<EntityId, Transaction> pair : result) {
            writer.deleteRow(pair.getKey());
        }
        System.out.println(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + " "
                + "Transactions were removed from sliding window: " + result.size());

        return Lists.transform(result, Pair<EntityId, Transaction>::getValue);
    }*/
}
