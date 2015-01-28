package com.my.sibyl.itemsets.util;
import java.util.List;

/**
 * Created by dkopiychenko on 1/28/15.
 */

//Stores list of all associations for the itemset
public class Associations {
    public List<String> itemSet;
    public List<String> associations;

    public Associations(List<String> itemSet, List<String> associations) {
        this.itemSet = itemSet;
        this.associations = associations;
    }

    @Override
    public String toString() {
        return "(" + itemSet +
                ", " + associations +
                ')';
    }
}
