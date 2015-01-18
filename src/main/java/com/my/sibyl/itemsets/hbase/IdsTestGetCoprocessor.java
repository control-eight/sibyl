package com.my.sibyl.itemsets.hbase;

import com.my.sibyl.itemsets.kiji.ItemSetsFields;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.CellSpec;

import java.io.IOException;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/16/15
 */
public class IdsTestGetCoprocessor extends BaseRegionObserver {

    /** Name of the table to read for ids entries. */
    public static final String TABLE_NAME = "ids";

    public static final Log LOG = LogFactory.getLog(IdsTestGetCoprocessor.class);

    //15/01/16 22:03:43 INFO com.my.sibyl.itemsets.hbase.IdsTestGetCoprocessor:
    //
    // d-9321284/B:B:B/1421435535986/Put/vlen=2/ts=0 //--// B //--// B:B //--// ^@ d-9321284^ABB:B^@^@^AJ�)^Br^D

    /*@Override
    public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
        LOG.info(results);
        for (KeyValue result : results) {
            LOG.info(result
                            + " //--// " + Bytes.toString(result.getFamily())
                            + " //--// " + Bytes.toString(result.getQualifier())
                            + " //--// " + Bytes.toString(result.getKey())
                            + " //--// " + Bytes.toInt(result.getValue())
            );
        }
        if(!results.isEmpty()) {
            results.remove(0);
        }
        //throw new AccessDeniedException("User is not allowed to access.");
    }*/

    @Override
    public void postGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {


        Kiji kiji = Kiji.Factory.open(KijiURI.newBuilder("kiji://.env/itemsets").build(),
                e.getEnvironment().getConfiguration());

        KijiTable srcTable = kiji.openTable(TABLE_NAME);
        //KijiTableReader reader = srcTable.openTableReader();
        HBaseKijiTable hTable = HBaseKijiTable.downcast(srcTable);
        //KijiColumnNameTranslator translator = hTable.getColumnNameTranslator();
        KijiColumnName columnName = new KijiColumnName(ItemSetsFields.INFO_FAMILY.getName(),
                ItemSetsFields.COUNT.getName());
        //HBaseColumnName hBaseColumnName = translator.toHBaseColumnName(columnName);
        CellSpec cellSpec = hTable.getLayoutCapsule().getLayout().getCellSpec(columnName);
        KijiCellDecoder decoder = cellSpec.getDecoderFactory().create(cellSpec);
        KeyValue keyValue = results.get(0);//get the column value
        Integer value = (Integer) decoder.decodeValue(keyValue.getValue());

        LOG.info(results);

        for (KeyValue result : results) {
            LOG.info(result
                            + " //--// " + Bytes.toString(result.getFamily())
                            + " //--// " + Bytes.toString(result.getQualifier())
                            + " //--// " + Bytes.toString(result.getKey())
                            + " //--// " + Bytes.toShort(result.getValue())
            );

            LOG.info("Value is: " + value);
        }
        /*if(!results.isEmpty()) {
            KeyValue keyValue = results.remove(0);

            //final byte[] row, final byte[] family,
            //final byte[] qualifier, final long timestamp, final byte[] value

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
            encoder.writeInt(124);
            encoder.flush();

            keyValue = new KeyValue(keyValue.getRow(), keyValue.getFamily(), keyValue.getQualifier(),
                    keyValue.getTimestamp(), os.toByteArray());
            results.add(keyValue);
        }*/
        //throw new AccessDeniedException("User is not allowed to access.");
    }

    /*@Override
    public void postGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
        LOG.info(results);

        for (KeyValue result : results) {
            LOG.info(result
                    + " //--// " + Bytes.toString(result.getFamily())
                    + " //--// " + Bytes.toString(result.getQualifier())
                    + " //--// " + Bytes.toString(result.getKey())
                    + " //--// " + Bytes.toShort(result.getValue())
            );
            Decoder decoder = DecoderFactory.get().binaryDecoder(result.getValue(), 0, result.getValue().length, null);
            LOG.info("Value is: " + decoder.readInt());
        }
        if(!results.isEmpty()) {
            KeyValue keyValue = results.remove(0);

            //final byte[] row, final byte[] family,
            //final byte[] qualifier, final long timestamp, final byte[] value

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
            encoder.writeInt(124);
            encoder.flush();

            keyValue = new KeyValue(keyValue.getRow(), keyValue.getFamily(), keyValue.getQualifier(),
                    keyValue.getTimestamp(), os.toByteArray());
            results.add(keyValue);
        }
        //throw new AccessDeniedException("User is not allowed to access.");
    }*/
}
