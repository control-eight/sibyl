package com.my.sibyl.itemsets;

import com.my.sibyl.itemsets.model.Instance;

/**
 * @author abykovsky
 * @since 1/28/15
 */
public interface InstancesService {

    public static final String DEFAULT = "default";

    void put(Instance instance);

    Instance get(String name);
}
