package com.my.sibyl.itemsets.rest.binding;

/**
 * @author abykovsky
 * @since 6/10/15
 */
public class InstanceStatus extends InstanceBinding {

    private long transactionsCount;

    public long getTransactionsCount() {
        return transactionsCount;
    }

    public void setTransactionsCount(long transactionsCount) {
        this.transactionsCount = transactionsCount;
    }

    @Override
    public String toString() {
        return "InstanceStatus{" +
                "name='" + getName() + '\'' +
                ", description='" + getDescription() + '\'' +
                ", measures=" + getMeasures() +
                ", dataLoadFiles=" + getDataLoadFiles() +
                ", startLoadDate=" + getStartLoadDate() +
                ", endLoadDate=" + getEndLoadDate() +
                ", slidingWindowSize=" + getSlidingWindowSize() +
                ", transactionsCount=" + transactionsCount +
                '}';
    }
}
