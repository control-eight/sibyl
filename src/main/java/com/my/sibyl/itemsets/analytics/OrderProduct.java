package com.my.sibyl.itemsets.analytics;

import com.my.sibyl.itemsets.legacy.MutableInteger;

/**
 * @author abykovsky
 * @since 12/3/14
 */
public class OrderProduct implements Comparable<OrderProduct> {

    private Long id;

    private MutableInteger quantity;

    public OrderProduct(Long id) {
        this.id = id;
        this.quantity = new MutableInteger(0);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public MutableInteger getQuantity() {
        return quantity;
    }

    public void setQuantity(MutableInteger quantity) {
        this.quantity = quantity;
    }

    @Override
    public int compareTo(OrderProduct o) {
        return o.quantity.getValue() - this.quantity.getValue();
    }
}
