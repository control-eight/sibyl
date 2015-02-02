package com.my.sibyl.itemsets.util;

import com.my.sibyl.itemsets.model.Instance;
import com.my.sibyl.itemsets.model.Transaction;
import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author abykovsky
 * @since 1/28/15
 */
public final class Avro {

    public static byte[] charSequenceTyBytes(final CharSequence charSequence) {
        try {
            try(ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
                encoder.writeString(charSequence);
                encoder.flush();
                return os.toByteArray();
            }
        } catch (IOException e) {
            throw new RuntimeException("Problems during serialization: " + e, e);
        }
    }

    public static byte[] instanceToBytes(Instance instance) {
        try {
            try(ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
                DatumWriter<Instance> userDatumWriter = new SpecificDatumWriter<>(Instance.class);
                userDatumWriter.write(instance, encoder);
                encoder.flush();
                return os.toByteArray();
            }
        } catch (IOException e) {
            throw new RuntimeException("Problems during serialization: " + e, e);
        }
    }

    public static Instance bytesToInstance(byte[] value) {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
            DatumReader<Instance> userDatumWriter = new SpecificDatumReader<>(Instance.class);
            return userDatumWriter.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException("Problems during serialization: " + e, e);
        }
    }

    /*public static void main(String[] args) {
        byte[] bytes = instanceToBytes(new Instance("bla", Collections.emptyList(), Collections.emptyList(), 10l));
        System.out.println(bytes);
        System.out.println(bytesToInstance(bytes));
    }*/

    public static byte[] transactionToBytes(Transaction transaction) {
        try {
            try(ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
                DatumWriter<Transaction> userDatumWriter = new SpecificDatumWriter<>(Transaction.class);
                userDatumWriter.write(transaction, encoder);
                encoder.flush();
                return os.toByteArray();
            }
        } catch (IOException e) {
            throw new RuntimeException("Problems during serialization: " + e, e);
        }
    }

    public static Transaction bytesToTransaction(byte[] value) {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
            DatumReader<Transaction> userDatumWriter = new SpecificDatumReader<>(Transaction.class);
            return userDatumWriter.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException("Problems during serialization: " + e, e);
        }
    }
}
