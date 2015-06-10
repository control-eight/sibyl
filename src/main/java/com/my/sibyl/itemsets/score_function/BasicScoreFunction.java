package com.my.sibyl.itemsets.score_function;

import com.my.sibyl.itemsets.model.Measure;
import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public class BasicScoreFunction implements ScoreFunction<Recommendation> {

    private List<Pair<Measure, Number>> thresholds;

    private List<Measure> sortParams;

    private List<Measure> outputParams;

    private int maxResults;

    public BasicScoreFunction(List<Pair<Measure, Number>> thresholds, List<Measure> sortParams, List<Measure> outputParams,
                              int maxResults) {
        this.thresholds = Collections.unmodifiableList(thresholds);
        this.sortParams = Collections.unmodifiableList(sortParams);
        this.outputParams = Collections.unmodifiableList(outputParams);
        this.maxResults = maxResults;
    }

    @Override
    public List<Pair<Measure, Number>> getThresholds() {
        return thresholds;
    }

    @Override
    public Number getThresholdValue(Measure measure) {
        return thresholds.stream().filter(pair -> pair.getKey() == measure).findAny().get().getValue();
    }

    @Override
    public boolean containsThresholdsMeasure(Measure measure) {
        return thresholds.stream().anyMatch(measureNumberPair -> measureNumberPair.getKey() == measure);
    }

    @Override
    public List<Measure> getSortParams() {
        return sortParams;
    }

    @Override
    public List<Measure> getOutputParams() {
        return outputParams;
    }

    @Override
    public int getMaxResults() {
        return maxResults;
    }

    @Override
    public String toString() {
        return "BasicScoreFunction{" +
                "thresholds=" + thresholds +
                ", sortParams=" + sortParams +
                ", outputParams=" + outputParams +
                ", maxResults=" + maxResults +
                '}';
    }
}
