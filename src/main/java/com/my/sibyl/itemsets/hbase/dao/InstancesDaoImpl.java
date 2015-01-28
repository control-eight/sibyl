package com.my.sibyl.itemsets.hbase.dao;

import com.my.sibyl.itemsets.dao.InstancesDao;

import com.my.sibyl.itemsets.model.Instance;
import com.my.sibyl.itemsets.util.Avro;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author abykovsky
 * @since 1/28/15
 */
public class InstancesDaoImpl implements InstancesDao {

    public static final byte[] TABLE_NAME = Bytes.toBytes("instances");
    public static final byte[] INFO_FAM = Bytes.toBytes("I");
    public static final byte[] CONFIGURATION_COLUMN = Bytes.toBytes("C");

    private HConnection connection;

    public InstancesDaoImpl(HConnection connection) {
        this.connection = connection;
    }

    @Override
    public void put(Instance instance) throws IOException {
        Put p = new Put(Avro.charSequenceTyBytes(instance.getName()));
        p.add(INFO_FAM, CONFIGURATION_COLUMN, Avro.instanceToBytes(instance));
        try(HTableInterface instances = connection.getTable(TABLE_NAME)) {
            instances.put(p);
        }
    }

    @Override
    public Instance get(String name) throws IOException {
        Get g = new Get(Avro.charSequenceTyBytes(name));
        g.addColumn(INFO_FAM, CONFIGURATION_COLUMN);
        try(HTableInterface instances = connection.getTable(TABLE_NAME)) {
            Result result = instances.get(g);
            byte[] value = result.getValue(INFO_FAM, CONFIGURATION_COLUMN);
            if(value != null) {
                return Avro.bytesToInstance(value);
            }
            return null;
        }
    }
}
