package com.my.sibyl.itemsets;

import com.google.common.collect.Lists;
import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl;
import com.my.sibyl.itemsets.score_function.ConfidenceRecommendationFilter;
import com.my.sibyl.itemsets.score_function.Recommendation;
import com.my.sibyl.itemsets.score_function.RecommendationFilter;
import com.my.sibyl.itemsets.score_function.ScoreFunction;
import com.my.sibyl.itemsets.score_function.ScoreFunctionResult;
import com.my.sibyl.itemsets.util.ItemSetsGenerator;
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
public class AssociationServiceImpl implements AssociationService {

    private static final Log LOG = LogFactory.getLog(AssociationServiceImpl.class);
    public static final String TRANSACTIONS_COUNT_ROW_KEY = " ";

    private int maxItemSetLength = (int) ConfigurationHolder.getConfiguration()
            .getInt("maxItemSetLength");

    private ItemSetsGenerator itemSetsGenerator = new ItemSetsGenerator(maxItemSetLength);

    private ItemSetsDao itemSetsDao;

    public AssociationServiceImpl() {

    }

    public AssociationServiceImpl(final HConnection connection) {
        itemSetsDao = new ItemSetsDaoImpl(connection);
    }

    public void setItemSetsDao(ItemSetsDao itemSetsDao) {
        this.itemSetsDao = itemSetsDao;
    }

    public void setItemSetsGenerator(ItemSetsGenerator itemSetsGenerator) {
        this.itemSetsGenerator = itemSetsGenerator;
    }

    @Override
    public void addTransaction(String instanceName, List<String> transactionItems) throws IOException {
        createItemSets(instanceName, transactionItems);
    }

    private void createItemSets(String instanceName, List<String> transactionItems) throws IOException {
        Collection<ItemSetAndAssociation<String>> itemSetAndAssociations = itemSetsGenerator
                .generateItemSetsAndAssociations(transactionItems, 1);
        LOG.debug("Generated itemSets: " + itemSetAndAssociations);
        updateItemSets(instanceName, itemSetAndAssociations);
        incrementTransactionsCount(instanceName);
    }

    private void incrementTransactionsCount(String instanceName) throws IOException {
        itemSetsDao.incrementItemSetCount(instanceName, TRANSACTIONS_COUNT_ROW_KEY, 1);
    }


    private void updateItemSets(String instanceName, Collection<ItemSetAndAssociation<String>> itemSetAndAssociations)
            throws IOException {
        for (ItemSetAndAssociation<String> itemSetAndAssociation : itemSetAndAssociations) {
            try {
                itemSetsDao.incrementItemSetAndAssociations(instanceName, itemSetAndAssociation.getItemSet(),
                        itemSetAndAssociation.getCount(), itemSetAndAssociation.getAssociationMap());
            } catch (HBaseException e) {
                LOG.error(e, e);
            }
        }
    }

    @Override
    public List<ScoreFunctionResult<String>> getRecommendations(String instanceName, List<String> basketItems,
                                                                        ScoreFunction<Recommendation> scoreFunction)
            throws IOException {
        List<String> itemSets = itemSetsGenerator.generateCombinations(basketItems);

        //get basic recommendations - itemsets, associations, frequency
        List<Recommendation> recommendationList = createBasicRecommendations(instanceName, itemSets);
        //filter phase, should be only one logic filter
        filterPhase(scoreFunction, recommendationList);
        //calculate different measures like lift, e.g. metrics
        calculateMeasures(instanceName, scoreFunction, recommendationList);
        //TODO should be only one logic filter
        //postCalculationMeasuresFilterPhase(scoreFunction, recommendationList);
        //sort
        Collections.sort(recommendationList, scoreFunction);
        //if there any results with equal association id we need to filter them and choose only with the greatest score
        recommendationList = filterDuplicates(recommendationList);
        //subtract using max results
        recommendationList = scoreFunction.cut(recommendationList);
        //create score function results with measures
        List<ScoreFunctionResult<String>> result = Lists.transform(recommendationList,
                input -> new ScoreFunctionResult<>(input.getAssociationId(), input.getConfidence(),
                        input.getLift()));

        return result;
    }

    private List<Recommendation> filterDuplicates(List<Recommendation> recommendationList) {
        Set<Recommendation> helpSet = new HashSet<>();
        for(Iterator<Recommendation> iter = recommendationList.iterator(); iter.hasNext();) {
            if(!helpSet.add(iter.next())) {
                iter.remove();
            }
        }
        return recommendationList;
    }

    private void calculateMeasures(String instanceName, ScoreFunction<Recommendation> scoreFunction,
                                   List<Recommendation> recommendationList)
            throws IOException {
        if(scoreFunction.isLiftInUse()) {
            calculateLift(instanceName, recommendationList);
        }
    }

    private void calculateLift(String instanceName, List<Recommendation> recommendationList) throws IOException {
        //TODO important place for performance
        Map<String, Long> assocCounts = itemSetsDao.getItemSetsCount(instanceName,
                new HashSet<>(Lists.transform(recommendationList, Recommendation::getAssociationId)));
        recommendationList.forEach(recommendation -> {
            Long count = assocCounts.get(recommendation.getAssociationId());
            if(count != null) recommendation.setCountOfAssociationAsItemSet(count);
        });

        long transactionsCount = itemSetsDao.getItemSetCount(instanceName, TRANSACTIONS_COUNT_ROW_KEY);
        for (Recommendation recommendation : recommendationList) {
            if(recommendation.getCountOfAssociationAsItemSet() == 0) continue;
            recommendation.setLift(recommendation.getAssociationCount() /
                    ((double)recommendation.getItemSetCount()
                            * recommendation.getCountOfAssociationAsItemSet()/transactionsCount));
            //divide by transactionCount
            //empty item-set => transactionCount
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

    private List<Recommendation> createBasicRecommendations(String instanceName, List<String> itemSets) throws IOException {
        List<Recommendation> recommendationList = new ArrayList<>();
        for (String itemSet : itemSets) {
            long itemSetCount = itemSetsDao.getItemSetCount(instanceName, itemSet);

            if(itemSetCount == 0) continue;

            Map<String, Long> associations = itemSetsDao.getAssociations(instanceName, itemSet);
            for (Map.Entry<String, Long> entry : associations.entrySet()) {
                Recommendation recommendation = new Recommendation();
                recommendation.setItemSet(itemSet);
                recommendation.setItemSetCount(itemSetCount);
                recommendation.setAssociationId(entry.getKey());
                recommendation.setAssociationCount(entry.getValue());
                recommendationList.add(recommendation);
            }
        }
        return recommendationList;
    }
}
