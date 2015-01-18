package com.my.sibyl.itemsets;

/**
 * @author abykovsky
 * @since 11/26/14
 */
public class GlobalScore implements Comparable<GlobalScore> {

    private Integer score;

    private Double confidence;

    public GlobalScore(Integer score, Double confidence) {
        this.score = score;
        this.confidence = confidence;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public Double getConfidence() {
        return confidence;
    }

    public void setConfidence(Double confidence) {
        this.confidence = confidence;
    }

    @Override
    public int compareTo(GlobalScore o) {
        double v = this.confidence - o.confidence;
        return v < 0? -1: v > 0? 1: 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GlobalScore that = (GlobalScore) o;

        if (score != null ? !score.equals(that.score) : that.score != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return score != null ? score.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "<" + score + "=(" + confidence + ")>";
    }
}
