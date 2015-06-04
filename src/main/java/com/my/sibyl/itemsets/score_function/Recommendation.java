package com.my.sibyl.itemsets.score_function;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public class Recommendation {

    //filling during creation of basic recommendations
    private String itemSet;

    private long itemSetCount;

    private String associationId;

    private long associationCount;

    private long countOfAssociationAsItemSet;

    //association rules measures
    private double support;

    private double confidence;

    private double lift;

    private double score;

    public String getItemSet() {
        return itemSet;
    }

    public void setItemSet(String itemSet) {
        this.itemSet = itemSet;
    }

    public long getItemSetCount() {
        return itemSetCount;
    }

    public void setItemSetCount(long itemSetCount) {
        this.itemSetCount = itemSetCount;
    }

    public String getAssociationId() {
        return associationId;
    }

    public void setAssociationId(String associationId) {
        this.associationId = associationId;
    }

    public long getAssociationCount() {
        return associationCount;
    }

    public void setAssociationCount(long associationCount) {
        this.associationCount = associationCount;
    }

    public long getCountOfAssociationAsItemSet() {
        return countOfAssociationAsItemSet;
    }

    public void setCountOfAssociationAsItemSet(long countOfAssociationAsItemSet) {
        this.countOfAssociationAsItemSet = countOfAssociationAsItemSet;
    }

    //TODO precision
    public double getSupport() {
        return support;
    }

    public void setSupport(double support) {
        this.support = support;
    }

    //TODO precision
    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }

    //TODO overflow issue!!!
    public double getLift() {
        return lift;
    }

    public void setLift(double lift) {
        this.lift = lift;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Recommendation that = (Recommendation) o;

        if (associationId != null ? !associationId.equals(that.associationId) : that.associationId != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return associationId != null ? associationId.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Recommendation{" +
                "itemSet='" + itemSet + '\'' +
                ", itemSetCount=" + itemSetCount +
                ", associationId='" + associationId + '\'' +
                ", associationCount=" + associationCount +
                ", support=" + support +
                ", confidence=" + confidence +
                ", lift=" + lift +
                ", score=" + score +
                '}';
    }
}
