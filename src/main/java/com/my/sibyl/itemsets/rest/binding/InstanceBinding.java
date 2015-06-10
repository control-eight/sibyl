package com.my.sibyl.itemsets.rest.binding;

import java.util.List;

/**
 * @author abykovsky
 * @since 5/29/15
 */
public class InstanceBinding {

    private String name;
    private String description;
    private List<String> measures;
    private List<String> dataLoadFiles;
    private long startLoadDate;
    private long endLoadDate;
    private long slidingWindowSize;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getMeasures() {
        return measures;
    }

    public void setMeasures(List<String> measures) {
        this.measures = measures;
    }

    public List<String> getDataLoadFiles() {
        return dataLoadFiles;
    }

    public void setDataLoadFiles(List<String> dataLoadFiles) {
        this.dataLoadFiles = dataLoadFiles;
    }

    public long getStartLoadDate() {
        return startLoadDate;
    }

    public void setStartLoadDate(long startLoadDate) {
        this.startLoadDate = startLoadDate;
    }

    public long getEndLoadDate() {
        return endLoadDate;
    }

    public void setEndLoadDate(long endLoadDate) {
        this.endLoadDate = endLoadDate;
    }

    public long getSlidingWindowSize() {
        return slidingWindowSize;
    }

    public void setSlidingWindowSize(long slidingWindowSize) {
        this.slidingWindowSize = slidingWindowSize;
    }

    @Override
    public String toString() {
        return "InstanceBinding{" +
                "name='" + name + '\'' +
                ", description=" + description +
                ", measures=" + measures +
                ", dataLoadFiles=" + dataLoadFiles +
                ", startLoadDate=" + startLoadDate +
                ", endLoadDate=" + endLoadDate +
                ", slidingWindowSize=" + slidingWindowSize +
                '}';
    }
}
