package com.my.sibyl.itemsets;

import com.my.sibyl.itemsets.rest.binding.TransactionBinding;
import com.my.sibyl.itemsets.score_function.Recommendation;
import com.my.sibyl.itemsets.score_function.ScoreFunction;
import com.my.sibyl.itemsets.score_function.ScoreFunctionResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author abykovsky
 * @since 1/27/15
 */
public interface AssociationService {

    //TODO change to using Transaction with all fields
    void addTransaction(String instanceName, List<String> transactionItems) throws IOException;

    void addTransactionBinding(String instanceName, TransactionBinding transaction) throws IOException;

    List<ScoreFunctionResult<String>> getRecommendations(String instanceName, List<String> basketItems,
                                                                 ScoreFunction<Recommendation> scoreFunction)
            throws IOException;

    long getTransactionsCount(String instanceName) throws IOException;

    Map<String, Long> getItemSetWithCountMore(String instanceName, long count) throws IOException;
}
