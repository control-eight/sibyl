package com.my.sibyl.itemsets.score_function;

import java.util.Comparator;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public interface ScoreFunction<T> extends Comparator<T> {

    List<RecommendationFilter> getRecommendationFilters();

    List<Recommendation> cut(List<Recommendation> recommendationList);

    boolean isLiftInUse();
}
