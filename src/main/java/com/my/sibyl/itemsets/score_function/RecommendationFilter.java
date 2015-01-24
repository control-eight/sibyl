package com.my.sibyl.itemsets.score_function;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public interface RecommendationFilter<T> {

    boolean filter(T value);
}
