package com.my.sibyl.itemsets.hbase.test;

import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.exceptions.HBaseException;

import java.io.IOException;

/**
 * @author abykovsky
 * @since 1/21/15
 */
public class TestItemSetsDao {

    public static void main(String[] args) throws IOException, HBaseException, InterruptedException {
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

            //new AssociationServiceImpl(connection).getRecommendations(Arrays.asList("1", "2", "3"), null);

            itemSetsDao.incrementItemSetCount(" ", 1);
        }
    }
}
