package com.my.sibyl.itemsets.hbase.test;

import com.my.sibyl.itemsets.hbase.dao.ItemSetsDao;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.exceptions.HBaseException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author abykovsky
 * @since 1/21/15
 */
public class TestItemSetsDao {

    public static void main(String[] args) throws IOException, HBaseException {
        Configuration myConf = HBaseConfiguration.create();

        try(HConnection connection = HConnectionManager.createConnection(myConf)) {
            ItemSetsDao itemSetsDao = new ItemSetsDao(connection);

            /*itemSetsDao.updateCount("1", 1);
            itemSetsDao.updateAssocCount("1", "2", 1);
            itemSetsDao.updateCount("2", 1);
            itemSetsDao.updateAssocCount("2", "1", 1);
            itemSetsDao.updateCount("1-2", 1);*/


            Map<String, Integer> assocMap3 = new HashMap<>();
            assocMap3.put("1", 1);
            assocMap3.put("2", 2);
            itemSetsDao.updateCounts("3", 3, assocMap3);

            System.out.println("3:" + itemSetsDao.getCount("3"));
            System.out.println("3=>1:" + itemSetsDao.getCount("3", "1"));
            System.out.println("3=>2:" + itemSetsDao.getCount("3", "2"));
        }
    }
}
