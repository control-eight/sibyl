package com.my.sibyl.itemsets.kiji.tests;

import com.my.sibyl.itemsets.kiji.ItemSetsFields;
import com.my.sibyl.itemsets.kiji.RecommendedProduct;
import com.my.sibyl.itemsets.kiji.RecommendedProducts;
import com.my.sibyl.itemsets.kiji.Score;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Interactively create a Candidate/Recommendation entry and add it to the Kiji table.
 * @author abykovsky
 * @since 12/21/14
 */
public class AddRecommendation extends Configured implements Tool {

    /** URI of Kiji instance to use (need to support Cassandra and HBase Kiji). */
    @SuppressWarnings("FieldCanBeLocal")
    @Flag(
            name="kiji",
            usage="Specify the Kiji instance containing the 'recommendations' table."
    )
    private String mKijiUri = "kiji://.env/default";

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

        // Interactively prompt the user for the record fields from the console.
        String itemSet = "4-5";
        /*RecommendedProducts recommendedProducts = new RecommendedProducts(Arrays.asList(
                new RecommendedProduct(1l, new Score(1, 0.1, 2)),
                new RecommendedProduct(2l, new Score(2, 0.2, 3)),
                new RecommendedProduct(3l, new Score(3, 0.3, 4))
        ));*/

        Kiji kiji = null;
        KijiTable table = null;
        KijiTableReader reader = null;
        AtomicKijiPutter putter = null;
        try {
            // Load HBase configuration before connecting to Kiji.
            setConf(HBaseConfiguration.addHbaseResources(getConf()));

            // Connect to Kiji and open the table.
            kiji = Kiji.Factory.open(KijiURI.newBuilder(mKijiUri).build(), getConf());
            table = kiji.openTable(TABLE_NAME);
            reader = table.openTableReader();
            putter = table.getWriterFactory().openAtomicPutter();

            // Create a row ID with itemSet.
            final EntityId itemSetId = table.getEntityId(itemSet);

            final KijiDataRequest request = KijiDataRequest.create(ItemSetsFields.INFO_FAMILY.getName(),
                    ItemSetsFields.RECOMMENDATIONS.getName());

            RecommendedProducts recommendedProducts = reader.get(itemSetId, request)
                    .getMostRecentValue(ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.RECOMMENDATIONS.getName());

            System.out.println(recommendedProducts);

            putter.begin(itemSetId);

            RecommendedProducts modifiedRecommendedProducts
                    = new RecommendedProducts(new ArrayList<>(recommendedProducts.getProducts()));

            RecommendedProduct recommendedProduct = modifiedRecommendedProducts.getProducts().get(0);
            RecommendedProduct element = new RecommendedProduct(recommendedProduct.getId(),
                    new Score(recommendedProduct.getScore().getFrequency(), recommendedProduct.getScore().getLift(),
                            recommendedProduct.getScore().getSuccessFrequency()));
            element.getScore().setLift(22.0);
            modifiedRecommendedProducts.getProducts().set(0, element);

            // Write the record fields to appropriate table columns in the row.
            // The column names are specified as constants in the Fields.java class.
            final long timestamp = System.currentTimeMillis();

            putter.put(ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.RECOMMENDATIONS.getName(), timestamp,
                    modifiedRecommendedProducts);

            boolean result = putter.checkAndCommit(ItemSetsFields.INFO_FAMILY.getName(),
                    ItemSetsFields.RECOMMENDATIONS.getName(), recommendedProducts);

            System.out.println("Result: " + result);

        } catch (KijiTableNotFoundException e) {
            System.out.println("Could not find Kiji table: " + TABLE_NAME);
            return 1;
        } finally {
            // Safely free up resources by closing in reverse order.
            ResourceUtils.closeOrLog(reader);
            ResourceUtils.closeOrLog(putter);
            ResourceUtils.releaseOrLog(table);
            ResourceUtils.releaseOrLog(kiji);
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
        System.exit(ToolRunner.run(new AddRecommendation(), args));
    }
}
