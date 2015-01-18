package com.my.sibyl.itemsets.kiji.tests;

import com.my.sibyl.itemsets.kiji.RecommendedProduct;
import com.my.sibyl.itemsets.kiji.Score;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.kiji.schema.util.ToJson;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author abykovsky
 * @since 12/21/14
 */
public class TestAvro {

    public static void main(String[] args) throws IOException {
        writeScore();
        getScore();
        writeMap();
        writeList();
        writeScore2();
    }

    private static void writeScore2() throws IOException {
        Score score = new Score(1, 0.1, 2);
        String str = ToJson.toAvroJsonString(score);
        System.out.println(str);
    }

    private static void writeScore() throws IOException {
        try(final FileOutputStream fileOutputStream = new FileOutputStream("example/score.json")) {
            final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(Score.SCHEMA$, fileOutputStream);
            final SpecificDatumWriter<Score> datumWriter = new SpecificDatumWriter<>(Score.SCHEMA$);

            Score score = new Score(1, 0.1, 2);
            datumWriter.write(score, encoder);

            encoder.flush();
        }

        DatumWriter<Score> userDatumWriter = new SpecificDatumWriter<>(Score.class);
        DataFileWriter<Score> dataFileWriter = new DataFileWriter<>(userDatumWriter);

        Score score = new Score(1, 0.1, 2);
        dataFileWriter.create(score.getSchema(), new File("example/score.avro"));
        dataFileWriter.append(score);
        dataFileWriter.close();
    }

    private static void getScore() throws IOException {
        try(final FileInputStream fileInputStream = new FileInputStream("example/score.json")) {
            final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(Score.SCHEMA$, fileInputStream);
            final SpecificDatumReader<Score> datumReader = new SpecificDatumReader<>(Score.SCHEMA$);
            Score score = datumReader.read(null, decoder);
            System.out.println("Read score: " + score);
        }
    }

    private static void writeMap() throws IOException {
        try(final FileOutputStream fileOutputStream = new FileOutputStream("example/map.json")) {
            Schema map = Schema.createMap(Score.SCHEMA$);
            final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(map, fileOutputStream);
            final SpecificDatumWriter<Map> datumWriter = new SpecificDatumWriter<>(map);

            Map<String, Score> scoreMap = new HashMap<>();
            scoreMap.put("10", new Score(1, 0.1, 2));
            scoreMap.put("20", new Score(4, 0.2, 5));
            datumWriter.write(scoreMap, encoder);

            encoder.flush();
        }
    }

    private static void writeList() throws IOException {
        try(final FileOutputStream fileOutputStream = new FileOutputStream("example/list.json")) {
            Schema array = Schema.createArray(RecommendedProduct.SCHEMA$);
            final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(array, fileOutputStream);
            final SpecificDatumWriter<List> datumWriter = new SpecificDatumWriter<>(array);

            List<RecommendedProduct> scoreList = new ArrayList<>();
            scoreList.add(new RecommendedProduct(1l, new Score(1, 0.1, 2)));
            scoreList.add(new RecommendedProduct(2l, new Score(4, 0.2, 5)));
            datumWriter.write(scoreList, encoder);

            encoder.flush();
        }
    }
}