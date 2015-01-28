package com.my.sibyl.itemsets;

import com.my.sibyl.itemsets.score_function.Recommendation;
import com.my.sibyl.itemsets.score_function.ScoreFunction;
import com.my.sibyl.itemsets.score_function.ScoreFunctionResult;

import java.io.IOException;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/27/15
 */
public interface AssociationService {

    void addTransaction(String instanceName, List<String> transactionItems) throws IOException;

    List<ScoreFunctionResult<String>> getRecommendations(String instanceName, List<String> basketItems,
                                                                 ScoreFunction<Recommendation> scoreFunction)
            throws IOException;
}
