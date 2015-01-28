package com.my.sibyl.itemsets;

import com.my.sibyl.itemsets.dao.InstancesDao;
import com.my.sibyl.itemsets.hbase.dao.InstancesDaoImpl;
import com.my.sibyl.itemsets.model.Instance;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;

import java.io.IOException;

/**
 * @author abykovsky
 * @since 1/28/15
 */
public class InstancesServiceImpl implements InstancesService {

    private static final Log LOG = LogFactory.getLog(InstancesServiceImpl.class);

    private InstancesDao instancesDao;

    public InstancesServiceImpl() {

    }

    public InstancesServiceImpl(final HConnection connection) {
        this.instancesDao = new InstancesDaoImpl(connection);
    }

    public void setInstancesDao(InstancesDao instancesDao) {
        this.instancesDao = instancesDao;
    }

    @Override
    public void put(Instance instance) {
        try {
            this.instancesDao.put(instance);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Instance get(String name) {
        try {
            return this.instancesDao.get(name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
