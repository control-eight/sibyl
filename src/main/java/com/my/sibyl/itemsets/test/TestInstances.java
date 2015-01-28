package com.my.sibyl.itemsets.test;

import com.my.sibyl.itemsets.InstancesService;
import com.my.sibyl.itemsets.InstancesServiceImpl;
import com.my.sibyl.itemsets.model.Instance;
import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/28/15
 */
public class TestInstances {

    public static void main(String[] args) throws IOException {
        Configuration myConf = HBaseConfiguration.create();

        try(HConnection connection = HConnectionManager.createConnection(myConf)) {
            InstancesService instancesService = new InstancesServiceImpl(connection);

            //addDefaultInstance(instancesService);
            System.out.println(instancesService.get("default"));
        }
    }

    private static void addDefaultInstance(InstancesService instancesService) {
        Instance defaultInstance = new Instance();
        defaultInstance.setName("default");
        defaultInstance.setDataLoadFiles(Collections.emptyList());
        List<CharSequence> measures = new ArrayList<>();
        measures.add("support");
        measures.add("confidence");
        measures.add("lift");
        defaultInstance.setMeasures(measures);
        defaultInstance.setSlidingWindowSize(0l);
        instancesService.put(defaultInstance);
    }
}
