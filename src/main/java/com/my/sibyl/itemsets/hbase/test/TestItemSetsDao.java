package com.my.sibyl.itemsets.hbase.test;

import com.my.sibyl.itemsets.hbase.dao.ItemSetsDao;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;

/**
 * @author abykovsky
 * @since 1/21/15
 */
public class TestItemSetsDao {

    public static void main(String[] args) throws IOException {
        Configuration myConf = HBaseConfiguration.create();

        try(HConnection connection = HConnectionManager.createConnection(myConf)) {
            ItemSetsDao itemSetsDao = new ItemSetsDao(connection);

            itemSetsDao.updateCount("1", 1);
            itemSetsDao.updateCount("1", "2", 1);
            itemSetsDao.updateCount("2", 1);
            itemSetsDao.updateCount("2", "1", 1);
            itemSetsDao.updateCount("1-2", 1);
        }
    }
}
