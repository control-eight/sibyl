package com.my.sibyl.itemsets.analytics;

import java.util.Date;

/**
 * @author abykovsky
 * @since 12/3/14
 */
public class Product {

    private long id;

    private Long masterProductId;

    private String category;

    private Date firstLiveDate;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getMasterProductId() {
        return masterProductId;
    }

    public void setMasterProductId(Long masterProductId) {
        this.masterProductId = masterProductId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        if(category == null) this.category = null;
        else this.category = category.intern();
    }

    public void setId(long id) {
        this.id = id;
    }

    public Date getFirstLiveDate() {
        return firstLiveDate;
    }

    public void setFirstLiveDate(Date firstLiveDate) {
        this.firstLiveDate = firstLiveDate;
    }
}
