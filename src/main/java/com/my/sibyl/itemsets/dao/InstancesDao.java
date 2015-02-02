package com.my.sibyl.itemsets.dao;

import com.my.sibyl.itemsets.model.Instance;

import java.io.IOException;

/**
 * @author abykovsky
 * @since 1/28/15
 */
public interface InstancesDao {

    void put(Instance instance) throws IOException;

    void delete(String name) throws IOException;

    Instance get(String name) throws IOException;
}
