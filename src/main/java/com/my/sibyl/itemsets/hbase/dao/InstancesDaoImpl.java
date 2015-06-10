package com.my.sibyl.itemsets.hbase.dao;

import com.my.sibyl.itemsets.dao.InstancesDao;

import com.my.sibyl.itemsets.model.Instance;
import com.my.sibyl.itemsets.util.Avro;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * @author abykovsky
 * @since 1/28/15
 */
@Singleton
public class InstancesDaoImpl implements InstancesDao {

    public static final byte[] TABLE_NAME = Bytes.toBytes("instances");
    public static final byte[] INFO_FAM = Bytes.toBytes("I");
    public static final byte[] CONFIGURATION_COLUMN = Bytes.toBytes("C");

    @Inject
    private HConnection connection;

    public InstancesDaoImpl() {
    }

    public InstancesDaoImpl(HConnection connection) {
        this.connection = connection;
    }

    @Override
    public void put(Instance instance) throws IOException {

        //instance name is empty, generate a new one
        if(instance.getName() == null || instance.getName().trim().isEmpty()) {
            generateRandomName(instance);
        }

        Put p = new Put(Avro.charSequenceTyBytes(instance.getName()));
        p.add(INFO_FAM, CONFIGURATION_COLUMN, Avro.instanceToBytes(instance));
        try(HTableInterface instances = connection.getTable(TABLE_NAME)) {
            instances.put(p);
        }
    }

    private void generateRandomName(Instance instance) throws IOException {
        String randomName = "";
        boolean exists = true;
        while (exists) {
            randomName = RandomStringUtils.randomAlphanumeric(10);
            exists = get(randomName) != null;
        }
        instance.setName(randomName);
    }

    @Override
    public void delete(String name) throws IOException {
        Delete d = new Delete(Avro.charSequenceTyBytes(name));
        d.deleteColumn(INFO_FAM, CONFIGURATION_COLUMN);
        try(HTableInterface instances = connection.getTable(TABLE_NAME)) {
            instances.delete(d);
        }
    }

    @Override
    public Instance get(String name) throws IOException {
        Get g = new Get(Avro.charSequenceTyBytes(name));
        g.addColumn(INFO_FAM, CONFIGURATION_COLUMN);
        try(HTableInterface instances = connection.getTable(TABLE_NAME)) {
            Result result = instances.get(g);
            if(result == null) return null;
            byte[] value = result.getValue(INFO_FAM, CONFIGURATION_COLUMN);
            if(value != null) {
                return Avro.bytesToInstance(value);
            }
            return null;
        }
    }
}
