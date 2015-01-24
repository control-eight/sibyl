package com.my.sibyl.itemsets;

import com.google.common.collect.Lists;
import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl;
import com.my.sibyl.itemsets.score_function.ConfidenceRecommendationFilter;
import com.my.sibyl.itemsets.score_function.Recommendation;
import com.my.sibyl.itemsets.score_function.RecommendationFilter;
import com.my.sibyl.itemsets.score_function.ScoreFunction;
import com.my.sibyl.itemsets.score_function.ScoreFunctionResult;
import com.my.sibyl.itemsets.util.CombinationsGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.exceptions.HBaseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 1/21/15
 */
public class AssociationServiceImpl {

    private static final Log LOG = LogFactory.getLog(AssociationServiceImpl.class);

    private int maxItemSetLength = (int) ConfigurationHolder.getConfiguration()
            .getInt("maxItemSetLength");

    //private PermutationsGenerator<String> permutationsGenerator = new PermutationsGenerator<>(maxItemSetLength);
    private CombinationsGenerator<String> combinationsGenerator = new CombinationsGenerator<>(maxItemSetLength);

    private ItemSetsDao itemSetsDao;

    public AssociationServiceImpl() {

    }

    public AssociationServiceImpl(final HConnection connection) {
        itemSetsDao = new ItemSetsDaoImpl(connection);
    }

    public void setItemSetsDao(ItemSetsDao itemSetsDao) {
        this.itemSetsDao = itemSetsDao;
    }

    public void setCombinationsGenerator(CombinationsGenerator<String> combinationsGenerator) {
        this.combinationsGenerator = combinationsGenerator;
    }

    public void processTransaction(List<String> transactionItems) throws IOException {
        processItemSets(transactionItems, 1);
    }

    private void processItemSets(List<String> transactionItems, int addAmount) throws IOException {
        Collection<ItemSetAndAssociation<String>> itemSetAndAssociations = combinationsGenerator
                .generateItemSetsAndAssociations(transactionItems, addAmount);
        LOG.debug("Generated itemSets: " + itemSetAndAssociations);
        updateItemSets(itemSetAndAssociations);
    }

    private void updateItemSets(Collection<ItemSetAndAssociation<String>> itemSetAndAssociations) throws IOException {
        for (ItemSetAndAssociation<String> itemSetAndAssociation : itemSetAndAssociations) {
            try {
                itemSetsDao.incrementItemSetAndAssociations(generateItemSetRowKey(itemSetAndAssociation.getItemSet()),
                        itemSetAndAssociation.getCount(), itemSetAndAssociation.getAssociationMap());
            } catch (HBaseException e) {
                LOG.error(e, e);
            }
        }
    }

    private String generateItemSetRowKey(Collection<String> itemSet) {
        StringBuilder result = new StringBuilder();

        Iterator<String> itemSetIterator = itemSet.iterator();
        result.append(itemSetIterator.next());
        while (itemSetIterator.hasNext()){
            result.append("-").append(itemSetIterator.next());
        }
        return result.toString();
    }

    public List<ScoreFunctionResult<String, Double>> getRecommendations(List<String> basketItems,
                                                                        ScoreFunction<Recommendation> scoreFunction)
            throws IOException {
        Collection<Set<String>> itemSets = combinationsGenerator.generateItemSets(basketItems);

        //get basic recommendations - itemsets, associations, frequency
        List<Recommendation> recommendationList = createBasicRecommendations(itemSets);
        //filter phase
        filterPhase(scoreFunction, recommendationList);
        //calculate different measures like lift
        calculateMeasures(scoreFunction, recommendationList);
        //sort
        Collections.sort(recommendationList, scoreFunction);
        //subtract using max results
        recommendationList = recommendationList.subList(0, Math.min(recommendationList.size(), scoreFunction.getMaxResults()));
        //calculate score
        recommendationList.forEach(scoreFunction::calculateScore);
        //if there any results with equal association id we need to filter them and choose only with the greatest score
        List<ScoreFunctionResult<String, Double>> result = filterDuplicates(recommendationList);

        return result;
    }

    private List<ScoreFunctionResult<String, Double>> filterDuplicates(List<Recommendation> recommendationList) {
        List<ScoreFunctionResult<String, Double>> transform = Lists.transform(recommendationList,
                input -> new ScoreFunctionResult<>(input.getAssociationId(), input.getScore()));
        Set<ScoreFunctionResult<String, Double>> resultSet = new HashSet<>();
        for (ScoreFunctionResult<String, Double> functionResult : transform) {
            resultSet.add(functionResult);
        }
        return new ArrayList<>(resultSet);
    }

    private void calculateMeasures(ScoreFunction<Recommendation> scoreFunction, List<Recommendation> recommendationList)
            throws IOException {
        if(scoreFunction.isLiftInUse()) {
            calculateLift(recommendationList);
        }
    }

    private void calculateLift(List<Recommendation> recommendationList) throws IOException {
        itemSetsDao.getCountsForAssociations(recommendationList);
        for (Recommendation recommendation : recommendationList) {
            recommendation.setLift((double) recommendation.getAssociationCount() /
                    (recommendation.getItemSetCount() * recommendation.getCountOfAssociationAsItemSet()));
        }
    }

    private void filterPhase(ScoreFunction<Recommendation> scoreFunction, List<Recommendation> recommendationList) {
        for (RecommendationFilter filter : scoreFunction.getRecommendationFilters()) {
            if(filter instanceof ConfidenceRecommendationFilter) {
                calculateConfidence(recommendationList);
                for(Iterator<Recommendation> iterator = recommendationList.iterator(); iterator.hasNext();) {
                    ConfidenceRecommendationFilter theFilter = (ConfidenceRecommendationFilter) filter;
                    if(theFilter.filter(iterator.next().getConfidence())) {
                        iterator.remove();
                    }
                }
            }
        }
    }

    private void calculateConfidence(List<Recommendation> recommendationList) {
        for (Recommendation recommendation : recommendationList) {
            recommendation.setConfidence((double) recommendation.getAssociationCount() / recommendation.getItemSetCount());
        }
    }

    private List<Recommendation> createBasicRecommendations(Collection<Set<String>> itemSets) throws IOException {
        List<Recommendation> recommendationList = new ArrayList<>();
        for (Collection<String> itemSet : itemSets) {
            String rowKey = generateItemSetRowKey(itemSet);
            long itemSetCount = itemSetsDao.getCount(rowKey);

            Map<String, Long> associations = itemSetsDao.getAssociations(rowKey);
            for (Map.Entry<String, Long> entry : associations.entrySet()) {
                Recommendation recommendation = new Recommendation();
                recommendation.setItemSet(rowKey);
                recommendation.setItemSetCount(itemSetCount);
                recommendation.setAssociationId(entry.getKey());
                recommendation.setAssociationCount(entry.getValue());
                recommendationList.add(recommendation);
            }
        }
        return recommendationList;
    }
}
