package com.my.sibyl.itemsets;

/**
 * @author abykovsky
 * @since 11/26/14
 */
public class Score implements Comparable<Score> {

    private Integer frequency;

    private Double confidence;

    private Integer successFrequency;

    private Integer score;

    public Score(Integer frequency, Double confidence) {
        this.frequency = frequency;
        this.confidence = confidence;
        this.successFrequency = 0;
        this.score = 0;
    }

    public Integer getFrequency() {
        return frequency;
    }

    public void setFrequency(Integer frequency) {
        this.frequency = frequency;
    }

    public Integer getSuccessFrequency() {
        return successFrequency;
    }

    public void setSuccessFrequency(Integer successFrequency) {
        this.successFrequency = successFrequency;
    }

    public Double getConfidence() {
        return confidence;
    }

    public void setConfidence(Double confidence) {
        this.confidence = confidence;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    @Override
    public int compareTo(Score o) {
        return this.score - o.score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Score score1 = (Score) o;

        if (score != null ? !score.equals(score1.score) : score1.score != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return score != null ? score.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "<" + score + "=(" + frequency + "+" + successFrequency + ":" + confidence + ")>";
    }
}
