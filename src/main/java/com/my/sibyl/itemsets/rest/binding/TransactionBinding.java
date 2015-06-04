package com.my.sibyl.itemsets.rest.binding;

import java.util.List;

/**
 * @author abykovsky
 * @since 5/27/15
 */
public class TransactionBinding {

    private String id;

    private List<String> items;

    private long createTimestamp;

    public TransactionBinding() {
    }

    public TransactionBinding(String id, List<String> items, long createTimestamp) {
        this.id = id;
        this.items = items;
        this.createTimestamp = createTimestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getItems() {
        return items;
    }

    public void setItems(List<String> items) {
        this.items = items;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    @Override
    public String toString() {
        return "TransactionBinding{" +
                "id='" + id + '\'' +
                ", items=" + items +
                ", createTimestamp=" + createTimestamp +
                '}';
    }
}
