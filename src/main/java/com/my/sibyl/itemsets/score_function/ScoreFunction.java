package com.my.sibyl.itemsets.score_function;

import com.my.sibyl.itemsets.model.Measure;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Comparator;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public interface ScoreFunction {

    List<Pair<Measure, Number>> getThresholds();

    Number getThresholdValue(Measure measure);

    boolean containsThresholdsMeasure(Measure measure);

    List<Measure> getSortParams();

    List<Measure> getOutputParams();

    int getMaxResults();
}
