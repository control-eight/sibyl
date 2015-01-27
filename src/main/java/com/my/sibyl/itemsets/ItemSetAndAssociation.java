package com.my.sibyl.itemsets;

import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public class ItemSetAndAssociation<T> {

    private String itemSet;

    private Long count;

    private Map<T, Long> associationMap;

    public String getItemSet() {
        return itemSet;
    }

    public void setItemSet(String itemSet) {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ItemSetAndAssociation that = (ItemSetAndAssociation) o;

        if (associationMap != null ? !associationMap.equals(that.associationMap) : that.associationMap != null)
            return false;
        if (count != null ? !count.equals(that.count) : that.count != null) return false;
        if (itemSet != null ? !itemSet.equals(that.itemSet) : that.itemSet != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = itemSet != null ? itemSet.hashCode() : 0;
        result = 31 * result + (count != null ? count.hashCode() : 0);
        result = 31 * result + (associationMap != null ? associationMap.hashCode() : 0);
        return result;
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
