package com.my.sibyl.itemsets;

/**
 * @author abykovsky
 * @since 11/24/14
 */
public class Container implements Comparable<Container> {

    private Long item;

    private GlobalScore globalScore;

    public Container(Long item, GlobalScore globalScore) {
        this.item = item;
        this.globalScore = globalScore;
    }

    public Long getItem() {
        return item;
    }

    public GlobalScore getGlobalScore() {
        return globalScore;
    }

    @Override
    public int compareTo(Container o) {
        return this.globalScore.compareTo(o.globalScore);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Container container = (Container) o;

        if (item != null ? !item.equals(container.item) : container.item != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return item != null ? item.hashCode() : 0;
    }
}
