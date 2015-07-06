package com.my.sibyl.itemsets.hadoop;

import com.my.sibyl.itemsets.ConfigurationHolder;
import com.my.sibyl.itemsets.hbase.dao.InstancesDaoImpl;
import com.my.sibyl.itemsets.model.Instance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author abykovsky
 * @since 6/19/15
 */
public class DriverHelper {

    public static Configuration initConf() {
        org.apache.commons.configuration.Configuration envConfiguration = ConfigurationHolder.getConfiguration();
        Configuration conf = HBaseConfiguration.create();

        for (Iterator<String> keyIter = envConfiguration.getKeys(); keyIter.hasNext();) {
            String key = keyIter.next();
            conf.set(key, envConfiguration.getString(key));
        }
        return conf;
    }

    public static Instance initInstance(String arg, Configuration conf) throws IOException {
        Instance instance;
        try(HConnection connection = HConnectionManager.createConnection(conf)) {
            instance = new InstancesDaoImpl(connection).get(arg);
        }

        if(instance == null) throw new RuntimeException("Instance \"" + arg + "\" isn't found!");
        return instance;
    }
}
