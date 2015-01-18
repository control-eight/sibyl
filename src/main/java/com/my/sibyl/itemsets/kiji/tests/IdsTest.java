package com.my.sibyl.itemsets.kiji.tests;

import com.my.sibyl.itemsets.kiji.ItemSetsFields;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRegion;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author abykovsky
 * @since 12/28/14
 */
public class IdsTest extends Configured implements Tool {

    /** URI of Kiji instance to use (need to support Cassandra and HBase Kiji). */
    @Flag(
            name="kiji",
            usage="Specify the Kiji instance containing the 'itemsets' table."
    )
    private String mKijiUri = "kiji://.env/default";

    /** Name of the table to read for ids entries. */
    public static final String TABLE_NAME = "ids";

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
        KijiRowScanner scanner = null;
        try {
            // Load HBase configuration before connecting to Kiji.
            setConf(HBaseConfiguration.addHbaseResources(getConf()));

            // Connect to Kiji and open the table.
            kiji = Kiji.Factory.open(KijiURI.newBuilder(mKijiUri).build(), getConf());
            table = kiji.openTable(TABLE_NAME);
            writer = table.openTableWriter();
            reader = table.openTableReader();

            Set<String> keys = new HashSet<>();
            for (KijiRegion kijiRegion : table.getRegions()) {
                String key = Bytes.toString(kijiRegion.getStartKey());
                if(!key.trim().isEmpty()) {
                    keys.add(key);
                }
                key = Bytes.toString(kijiRegion.getEndKey());
                if(!key.trim().isEmpty()) {
                    keys.add(key);
                }
            }
            System.out.println(keys);
            List<String> keysList = new ArrayList<>(keys);

            // Create a row ID with itemSet.
            // The column names are specified as constants in the Fields.java class.
            EntityId entityId = put(table, writer, "8754321", 1, keysList);
            put(table, writer, "2326943", 2, keysList);
            put(table, writer, "1236548", 3, keysList);
            put(table, writer, "2123357", 4, keysList);
            put(table, writer, "9321284", 5, keysList);


            /*int count = 5;
            List<Long> intervals = new ArrayList<>();
            List<String> keysList = new ArrayList<>(keys);
            for(int i = 0; i < 5_000_000; i++) {
                long id = System.nanoTime();
                put(table, writer, id, i, keysList);
                if(i % 50000 == 0) {
                    System.out.println(50000);
                }
                if(i % 10 == 0 && count > 0) {
                    System.out.println(id);
                    intervals.add(id);
                    count--;
                }
            }*/

            /*for(String value : Arrays.asList("8754321", "2326943", "1236548", "2123357", "9321284", Integer.MAX_VALUE+"", 1_544_123_000+"")) {
                System.out.println("---");
                System.out.println(table.getEntityId(value));
                System.out.println("---");
                System.out.println(table.getEntityId(value).toShellString());
                System.out.println("---");
                System.out.println(Hex.encodeHexString(table.getEntityId(value).getHBaseRowKey()));
                System.out.println("---");
                System.out.println(ByteArrayFormatter.toHex(table.getEntityId(value).getHBaseRowKey()));
                System.out.println("---");
                System.out.println(table.getEntityId(value).getComponents());
                System.out.println("---");
            }*/

            final KijiDataRequest dataRequest = KijiDataRequest.builder()
                    .addColumns(KijiDataRequestBuilder.ColumnsDef.create().add(ItemSetsFields.INFO_FAMILY.getName(),
                            ItemSetsFields.COUNT.getName())).build();
            /*final KijiTableReader.KijiScannerOptions scanOptions = new KijiTableReader.KijiScannerOptions()
                    .setStartRow(table.getEntityId("aaa-1236548"))
                    //.setStartRow(table.getEntityId(8754321))
                    .setStopRow(table.getEntityId("zzz-9321284"));*/
                    //.setStopRow(table.getEntityId(9321284));

            /*

1420828636212519000
1420828636240644000
1420828636247757000
1420828636253593000
1420828636260505000
             */

            //List<Long> intervals = Arrays.asList(1420817907668643000l, 1420817908092845000l);
            //List<Long> intervals = Arrays.asList(1420828636253593000l, 1420828636260505000l);

            KijiRowData kijiRowData = reader.get(entityId, dataRequest);

            /*Schema schema = kijiRowData.getReaderSchema(ItemSetsFields.INFO_FAMILY.getName(),
                    ItemSetsFields.COUNT.getName());
            System.out.println(schema.);*/

            Integer mostRecentValue = kijiRowData.getMostRecentValue(ItemSetsFields.INFO_FAMILY.getName(),
                    ItemSetsFields.COUNT.getName());

            /*Method m = kijiRowData.getClass().getMethod("getHBaseResult");
            byte[] value = ((Result) m.invoke(kijiRowData)).getValue(Bytes.toBytes("B"), Bytes.toBytes("B:B"));
            Decoder decoder = DecoderFactory.get().binaryDecoder(value, 0, value.length, null);
            System.out.println(decoder.readInt());*/

            System.out.println(mostRecentValue);

            /*for (String prefix : keys) {
                final KijiTableReader.KijiScannerOptions scanOptions = new KijiTableReader.KijiScannerOptions()
                        .setStartRow(table.getEntityId(prefix + "-" + intervals.get(0)))
                                //.setStartRow(table.getEntityId(8754321))
                        .setStopRow(table.getEntityId(prefix + "-" + intervals.get(1)));

                long start = System.currentTimeMillis();
                scanner = reader.getScanner(dataRequest, scanOptions);
                long end = System.currentTimeMillis();
                System.out.println("Time1: " + (end - start) + "ms");
                // Scan over the requested row range, in order:
                long start1 = System.currentTimeMillis();
                int i = 0;
                for (KijiRowData row : scanner) {
                    // Process the row:
                    //System.out.println(row);
                    //System.out.println(row.getEntityId().getClass());
                    i++;
                }
                long end1 = System.currentTimeMillis();
                System.out.println("Time2: " + (end1 - start1) + "ms" + " Count: " + i);
            }*/
        } catch (KijiTableNotFoundException e) {
            System.out.println("Could not find Kiji table: " + TABLE_NAME);
            return 1;
        } finally {
            // Safely free up resources by closing in reverse order.
            //ResourceUtils.closeOrLog(scanner);
            ResourceUtils.closeOrLog(reader);
            ResourceUtils.closeOrLog(writer);
            ResourceUtils.releaseOrLog(table);
            ResourceUtils.releaseOrLog(kiji);
        }

        return 0;
    }

    private String generateShardKey() {
        return RandomStringUtils.randomAlphabetic(3).toLowerCase() + "-";
    }

    private String generateShardKey(List<String> keys) {
        return keys.get((int) (Math.random() * keys.size())) + "-";
    }

    private EntityId put(KijiTable table, KijiTableWriter writer, Object i, int j) throws java.io.IOException {
        final long timestamp = System.currentTimeMillis();
        EntityId entityId = table.getEntityId(generateShardKey() + i);
        writer.put(entityId, ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.COUNT.getName(), timestamp, j);
        return entityId;
    }

    private EntityId put(KijiTable table, KijiTableWriter writer, Object i, int j, List<String> keys) throws java.io.IOException {
        final long timestamp = System.currentTimeMillis();
        EntityId entityId = table.getEntityId(generateShardKey(keys) + i);
        writer.put(entityId, ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.COUNT.getName(), timestamp, j);
        return entityId;
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
        System.exit(ToolRunner.run(new IdsTest(), args));
    }
}
