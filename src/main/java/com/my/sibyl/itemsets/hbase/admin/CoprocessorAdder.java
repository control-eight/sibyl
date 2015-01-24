package com.my.sibyl.itemsets.hbase.admin;

import com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;

/**
 * @author abykovsky
 * @since 1/22/15
 */
public class CoprocessorAdder {

    public static void main(String[] args) throws IOException {
        Configuration myConf = HBaseConfiguration.create();

        /*try(HConnection connection = HConnectionManager.createConnection(myConf)) {
            HBaseAdmin admin = new HBaseAdmin(connection);
            admin.getTableDescriptor(ItemSetsDaoImpl.TABLE_NAME).addCoprocessor();
        }*/
    }
}
