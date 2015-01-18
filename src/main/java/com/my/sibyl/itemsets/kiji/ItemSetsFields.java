package com.my.sibyl.itemsets.kiji;

/**
 * @author abykovsky
 * @since 12/20/14
 */
public enum ItemSetsFields {

    INFO_FAMILY("info"),
    ID("itemsets_id"),
    COUNT("count"),
    RECOMMENDATIONS("recommendations"),
    TRANSACTION("transaction")
    ;

    private String name;

    private ItemSetsFields(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
