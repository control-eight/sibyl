package com.my.sibyl.itemsets;

import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public class ItemSetAndAssociation<T> {

    private Set<T> itemSet;

    private Long count;

    private Map<T, Long> associationMap;

    public Set<T> getItemSet() {
        return itemSet;
    }

    public void setItemSet(Set<T> itemSet) {
        this.itemSet = itemSet;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Map<T, Long> getAssociationMap() {
        return associationMap;
    }

    public void setAssociationMap(Map<T, Long> associationMap) {
        this.associationMap = associationMap;
    }

    @Override
    public String toString() {
        return "ItemSetAndAssociation{" +
                "itemSet=" + itemSet +
                ", count=" + count +
                ", associationMap=" + associationMap +
                '}';
    }
}
