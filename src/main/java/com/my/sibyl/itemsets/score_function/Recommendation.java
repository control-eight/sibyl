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

    public double getSupport() {
        return support;
    }

    public void setSupport(double support) {
        this.support = support;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }

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

        if (associationCount != that.associationCount) return false;
        if (Double.compare(that.confidence, confidence) != 0) return false;
        if (countOfAssociationAsItemSet != that.countOfAssociationAsItemSet) return false;
        if (itemSetCount != that.itemSetCount) return false;
        if (Double.compare(that.lift, lift) != 0) return false;
        if (Double.compare(that.score, score) != 0) return false;
        if (Double.compare(that.support, support) != 0) return false;
        if (associationId != null ? !associationId.equals(that.associationId) : that.associationId != null)
            return false;
        if (itemSet != null ? !itemSet.equals(that.itemSet) : that.itemSet != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = itemSet != null ? itemSet.hashCode() : 0;
        result = 31 * result + (int) (itemSetCount ^ (itemSetCount >>> 32));
        result = 31 * result + (associationId != null ? associationId.hashCode() : 0);
        result = 31 * result + (int) (associationCount ^ (associationCount >>> 32));
        result = 31 * result + (int) (countOfAssociationAsItemSet ^ (countOfAssociationAsItemSet >>> 32));
        temp = Double.doubleToLongBits(support);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(confidence);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(lift);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(score);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
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
