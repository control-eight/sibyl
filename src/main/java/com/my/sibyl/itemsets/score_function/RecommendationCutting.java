package com.my.sibyl.itemsets.score_function;

import java.util.List;

/**
 * @author abykovsky
 * @since 1/27/15
 */
public interface RecommendationCutting {

    void cut(List<Recommendation> recommendationList);
}
