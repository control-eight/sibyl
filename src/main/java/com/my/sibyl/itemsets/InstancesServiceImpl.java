package com.my.sibyl.itemsets;

import com.my.sibyl.itemsets.dao.InstancesDao;
import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.hbase.dao.InstancesDaoImpl;
import com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl;
import com.my.sibyl.itemsets.model.Instance;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.exceptions.HBaseException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * @author abykovsky
 * @since 1/28/15
 */
@Singleton
public class InstancesServiceImpl implements InstancesService {

    private static final Log LOG = LogFactory.getLog(InstancesServiceImpl.class);

    @Inject
    private InstancesDao instancesDao;

    @Inject
    private ItemSetsDao itemSetsDao;

    public InstancesServiceImpl() {

    }

    public InstancesServiceImpl(final HConnection connection) {
        this.instancesDao = new InstancesDaoImpl(connection);
        this.itemSetsDao = new ItemSetsDaoImpl(connection);
    }

    public void setInstancesDao(InstancesDao instancesDao) {
        this.instancesDao = instancesDao;
    }

    public void setItemSetsDao(ItemSetsDao itemSetsDao) {
        this.itemSetsDao = itemSetsDao;
    }

    @Override
    public void createInstance(Instance instance) {
        try {
            this.instancesDao.put(instance);
            this.itemSetsDao.createTable(instance.getName());
        } catch (IOException | HBaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteInstance(String name) {
        try {
            this.instancesDao.delete(name);
            this.itemSetsDao.deleteTable(name);
        } catch (IOException | HBaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Instance getInstance(String name) {
        try {
            return this.instancesDao.get(name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
