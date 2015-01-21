package com.my.sibyl.itemsets.api;

import java.util.List;
import java.util.function.Function;

/**
 * It's not remote interface.
 * Real-time ItemSets (associations) based recommendations
 * @author abykovsky
 * @since 1/15/15
 */
public interface RecommendationService {

    /**
     * TODO check duplicates
     * TODO check size
     * Add new transaction to recommendation system. Updates sliding window and recommendations
     * @param transactionItems
     */
    public void addTransaction(List<Long> transactionItems);

    /**
     * TODO check duplicates
     * TODO check size
     * Remove transaction from recommendation system. Updates sliding window and recommendations
     * @param transactionItems
     */
    public void removeTransaction(List<Long> transactionItems);

    /**
     * TODO check duplicates
     * TODO check size
     * Gets sorted recommended items for itemId
     *
     */
    public List<Long> getRecommendations(List<Long> basketItems,
                                         Function<List<Long>, List<Long>> scoreFunction);
}
