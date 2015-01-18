package com.my.sibyl.itemsets.kiji.tests;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author abykovsky
 * @since 12/22/14
 */
public class AvroUtils {

    public static <V extends IndexedRecord> String avroToJson(V record) {
        try(ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), baos);
            final DatumWriter<V> datumWriter = new SpecificDatumWriter<>(record.getSchema());
            datumWriter.write(record, encoder);
            encoder.flush();
            baos.flush();
            return baos.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <V extends IndexedRecord> String avroToJsonData(V record) {
        try(ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final DatumWriter<V> datumWriter = new SpecificDatumWriter<>(record.getSchema());
            DataFileWriter<V> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(record.getSchema(), baos);
            dataFileWriter.append(record);
            dataFileWriter.close();
            baos.flush();
            return baos.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
