package com.my.sibyl.itemsets.test;

import com.my.sibyl.itemsets.AssociationServiceImpl;
import com.my.sibyl.itemsets.InstancesService;
import com.my.sibyl.itemsets.InstancesServiceImpl;
import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl;
import com.my.sibyl.itemsets.hbase.dao.TransactionsDaoImpl;
import com.my.sibyl.itemsets.model.Instance;
import com.my.sibyl.itemsets.model.Measure;
import com.my.sibyl.itemsets.model.Transaction;
import com.my.sibyl.itemsets.score_function.BasicScoreFunction;
import com.my.sibyl.itemsets.score_function.ConfidenceRecommendationFilter;
import com.my.sibyl.itemsets.score_function.Recommendation;
import com.my.sibyl.itemsets.score_function.ScoreFunction;
import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.exceptions.HBaseException;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/21/15
 */
public class TestItemSetsDao {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) throws IOException, HBaseException, InterruptedException, ParseException {
        Configuration myConf = HBaseConfiguration.create();

        try(HConnection connection = HConnectionManager.createConnection(myConf)) {
            ItemSetsDao itemSetsDao = new ItemSetsDaoImpl(connection);

            /*itemSetsDao.updateItemSetCount("1", 1);
            itemSetsDao.updateAssocCount("1", "2", 1);
            itemSetsDao.updateItemSetCount("2", 1);
            itemSetsDao.updateAssocCount("2", "1", 1);
            itemSetsDao.updateItemSetCount("1-2", 1);*/


            /*Map<String, Integer> assocMap3 = new HashMap<>();
            assocMap3.put("1", 1);
            assocMap3.put("2", 2);
            itemSetsDao.updateItemSetsCount("3", 3, assocMap3);

            System.out.println("3:" + itemSetsDao.getItemSetCount("3"));
            System.out.println("3=>1:" + itemSetsDao.getItemSetCount("3", "1"));
            System.out.println("3=>1:" + itemSetsDao.getItemSetCount("3", "1"));*/

            /*System.out.println(itemSetsDao.incrementItemSetCount("1-2", 1));
            System.out.println(itemSetsDao.incrementAssocCount("1", "2", 1));*/

            //new AssociationServiceImpl(connection).addTransaction(Arrays.asList("1", "2"));
            //new AssociationServiceImpl(connection).addTransaction(Arrays.asList("1", "2", "3"));
            //new AssociationServiceImpl(connection).addTransaction(Arrays.asList("1", "2", "3", "4"));

            //System.out.println(itemSetsDao.getAssociations("1"));

            /*Map<String, Long> map = new HashMap<>();
            for(int i = 0; i < 10000; i++) {
                map.put(i + "", (long) i);
            }
            itemSetsDao.updateItemSetsCount("test", 1, map);

            TestStore s = new TestStore();
            s.set(itemSetsDao.getAssociations("test"));
            Thread.sleep(50000000);*/

            /*for (int i = 0; i < 10; i++) {
                long start = System.currentTimeMillis();
                getRecommendations(connection);
                System.out.println((System.currentTimeMillis() - start) + "ms");
            }*/

            //itemSetsDao.incrementItemSetCount(InstancesService.DEFAULT, AssociationServiceImpl.TRANSACTIONS_COUNT_ROW_KEY, 1694868)

            //System.out.println(itemSetsDao.getItemSetCount(InstancesService.DEFAULT,
            //        AssociationServiceImpl.TRANSACTIONS_COUNT_ROW_KEY));

            //itemSetsDao.incrementItemSetCount(DEFAULT, " ", 1);

            //scanTransactions(connection);

            /*new InstancesServiceImpl(connection).deleteInstance(InstancesService.DEFAULT);*/

            //createInstance(connection);

            System.out.println(new InstancesServiceImpl(connection).getInstance(InstancesService.DEFAULT));

            //1694868
            //scanTransactions(connection, "2013-10-16", "2013-11-16");
            //scanTransactions(connection, "2000-01-01", "2000-01-02");

            //addTransactions(connection);

            //System.out.println(new ItemSetsDaoImpl(connection).getItemSetWithCountMore(InstancesService.DEFAULT, 1000));
        }
    }

    private static void addTransactions(HConnection connection) throws ParseException, IOException {
        Transaction transaction = new Transaction();
        transaction.setId("-1");
        transaction.setItems(Arrays.asList("1", "2", "3"));
        transaction.setQuantities(Collections.emptyList());
        transaction.setCreateTimestamp(DATE_FORMAT.parse("2000-01-01").getTime());
        new TransactionsDaoImpl(connection).addTransaction(transaction);

        transaction = new Transaction();
        transaction.setId("-2");
        transaction.setItems(Arrays.asList("1", "2"));
        transaction.setQuantities(Collections.emptyList());
        transaction.setCreateTimestamp(DATE_FORMAT.parse("2000-01-01").getTime());
        new TransactionsDaoImpl(connection).addTransaction(transaction);

        transaction = new Transaction();
        transaction.setId("-3");
        transaction.setItems(Arrays.asList("2", "3"));
        transaction.setQuantities(Collections.emptyList());
        transaction.setCreateTimestamp(DATE_FORMAT.parse("2000-01-01").getTime());
        new TransactionsDaoImpl(connection).addTransaction(transaction);
    }

    private static void createInstance(HConnection connection) throws ParseException {
        Instance instance = new Instance();
        /*instance.setName(InstancesService.DEFAULT);
        instance.setStartLoadDate(DATE_FORMAT.parse("2013-10-16").getTime());
        instance.setEndLoadDate(DATE_FORMAT.parse("2013-11-16").getTime());*/

        instance.setName("test");
        instance.setStartLoadDate(DATE_FORMAT.parse("2000-01-01").getTime());
        instance.setEndLoadDate(DATE_FORMAT.parse("2000-01-02").getTime());

        instance.setMeasures(Measure.stringValues());

        instance.setDataLoadFiles(Collections.emptyList());
        instance.setSlidingWindowSize(-1l);

        new InstancesServiceImpl(connection).createInstance(instance);
    }

    private static void scanTransactions(HConnection connection, String startDate, String endDate)
            throws ParseException, IOException {
        /*System.out.println(new Date(1381881600000l));
        System.out.println(dateFormat.parse("16-OCT-13").getTime());*/

        List<Transaction> transactionList = new TransactionsDaoImpl(connection)
                .scanTransactions(DATE_FORMAT.parse(startDate), DATE_FORMAT.parse(endDate));
        System.out.println(transactionList.size());
    }

    private static void getRecommendations(HConnection connection) throws IOException {
        boolean isLiftInUse = true;
        double confidence = 0.0005;
        int maxResults = 10;
        ScoreFunction<Recommendation> scoreFunction = new BasicScoreFunction(maxResults,
                Arrays.asList(new ConfidenceRecommendationFilter() {
                    @Override
                    public boolean filter(Double value) {
                        return value < confidence;
                    }
                }), isLiftInUse);

        System.out.println(new AssociationServiceImpl(connection)
                .getRecommendations(InstancesService.DEFAULT, makeBasketItems2(), scoreFunction));
    }

    private static List<String> makeBasketItems() {
        return Arrays.asList("1", "2", "3");
    }

    private static List<String> makeBasketItems2() {
        return Arrays.asList("451907", "948681", "857932");
    }

    private static List<String> makeBasketItems3() {
        return Arrays.asList("1100219", "1010968", "451907");
    }
}
