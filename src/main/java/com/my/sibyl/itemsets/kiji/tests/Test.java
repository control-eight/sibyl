package com.my.sibyl.itemsets.kiji.tests;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author abykovsky
 * @since 12/27/14
 */
public class Test {

    public static void main(String[] args) throws IOException, URISyntaxException {
        /*Random r = new Random(309022150);
        for(int i = 0; i < 10; i++) {
            int value = r.nextInt(1_000_000_000);
            System.out.println(value);
            r = new Random(value);
        }*/
        //long start = System.currentTimeMillis();
        //MersenneTwister r = new MersenneTwister(146084203);
        /*for(int i = 0; i < 10; i++) {
            int value = r.nextInt(1_000_000_000);
            System.out.println(value);
            r = new MersenneTwister(value);
        }*/
        //System.out.println(System.currentTimeMillis() - start);

        /*Configuration configuration = new Configuration();
        DistributedFileSystem distributedFileSystem = new DistributedFileSystem();
        distributedFileSystem.initialize(new URI("hdfs://localhost"), new Configuration());
        RemoteIterator<LocatedFileStatus> iter = distributedFileSystem.listFiles(new Path("/"), true);
        if(iter.hasNext()) {
            LocatedFileStatus next = iter.next();
            System.out.println(next.getPath());
        }
        FileStatus fileStatus = distributedFileSystem.getFileStatus(new Path("hdfs://localhost/user/abykovsky/sibyl.jar"));
        System.out.println(fileStatus.getPath());*/

        System.out.println(Bytes.toInt(Bytes.toBytes(100)));

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
        encoder.writeInt(124);
        encoder.flush();
        System.out.println(Bytes.toString(os.toByteArray()));

        Decoder decoder = DecoderFactory.get().binaryDecoder(os.toByteArray(), 0, os.toByteArray().length, null);
        System.out.println(decoder.readInt());

        //new AvroCellEncoder()
    }
}
