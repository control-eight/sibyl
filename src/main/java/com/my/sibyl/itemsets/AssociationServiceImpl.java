package com.my.sibyl.itemsets;

import com.google.common.collect.Lists;
import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.dao.TransactionsDao;
import com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl;
import com.my.sibyl.itemsets.hbase.dao.TransactionsDaoImpl;
import com.my.sibyl.itemsets.model.Measure;
import com.my.sibyl.itemsets.model.Transaction;
import com.my.sibyl.itemsets.rest.binding.TransactionBinding;
import com.my.sibyl.itemsets.score_function.Recommendation;
import com.my.sibyl.itemsets.score_function.ScoreFunction;
import com.my.sibyl.itemsets.score_function.ScoreFunctionResult;
import com.my.sibyl.itemsets.util.ItemSetsGenerator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.exceptions.HBaseException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 1/21/15
 */
@Singleton
public class AssociationServiceImpl implements AssociationService {

    private static final Log LOG = LogFactory.getLog(AssociationServiceImpl.class);
    public static final String TRANSACTIONS_COUNT_ROW_KEY = " ";

    private int maxItemSetLength = (int) ConfigurationHolder.getConfiguration()
            .getInt("maxItemSetLength");

    private ItemSetsGenerator itemSetsGenerator = new ItemSetsGenerator(maxItemSetLength);

    @Inject
    private ItemSetsDao itemSetsDao;

    @Inject
    private TransactionsDao transactionsDao;

    public AssociationServiceImpl() {

    }

    public AssociationServiceImpl(final HConnection connection) {
        itemSetsDao = new ItemSetsDaoImpl(connection);
        transactionsDao = new TransactionsDaoImpl(connection);
    }

    public void setItemSetsDao(ItemSetsDao itemSetsDao) {
        this.itemSetsDao = itemSetsDao;
    }

    public void setTransactionsDao(TransactionsDao transactionsDao) {
        this.transactionsDao = transactionsDao;
    }

    public void setItemSetsGenerator(ItemSetsGenerator itemSetsGenerator) {
        this.itemSetsGenerator = itemSetsGenerator;
    }

    @Override
    public void addTransaction(String instanceName, List<String> transactionItems) throws IOException {
        createItemSets(instanceName, transactionItems);
    }

    @Override
    public void addTransactionBinding(String instanceName, TransactionBinding transaction) throws IOException {
        //TODO
        Transaction transactionModel = new Transaction();
        transactionModel.setId(transaction.getId());
        transactionModel.setItems(transaction.getItems());
        transactionModel.setQuantities(Collections.emptyList());
        transactionModel.setCreateTimestamp(transaction.getCreateTimestamp());

        transactionsDao.addTransaction(instanceName, transactionModel);
        createItemSets(instanceName, transaction.getItems());
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
        long transactionsCount = getTransactionsCount(instanceName);

        List<String> itemSets = itemSetsGenerator.generateCombinations(basketItems);

        Set<Measure> loadedMeasures = new HashSet<>();
        loadedMeasures.add(Measure.COUNT);

        //get basic recommendations - itemsets, associations, frequency
        List<Recommendation> recommendationList = createBasicRecommendations(instanceName, itemSets, scoreFunction);

        //filter phase, should be only one logic filter
        filterPhase(instanceName, scoreFunction, recommendationList, transactionsCount, loadedMeasures);
        //sort
        sortRecommendations(instanceName, scoreFunction, recommendationList, transactionsCount, loadedMeasures);
        //if there any results with equal association id we need to filter them and choose only with the greatest score
        recommendationList = filterDuplicates(recommendationList);
        //subtract using max results
        recommendationList = recommendationList.subList(0, Math.min(recommendationList.size(), scoreFunction.getMaxResults()));
        //create score function results with measures
        checkRequiredMeasuresForOutput(instanceName, scoreFunction, recommendationList, transactionsCount, loadedMeasures);
        List<ScoreFunctionResult<String>> result = Lists.transform(recommendationList,
                input -> createScoreFunctionResult(input, loadedMeasures));

        return result;
    }

    private void checkRequiredMeasuresForOutput(String instanceName, ScoreFunction<Recommendation> scoreFunction,
                                                List<Recommendation> recommendationList, long transactionsCount,
                                                Set<Measure> loadedMeasures) throws IOException {
        for (Measure measure : scoreFunction.getOutputParams()) {
            loadSupportIfRequired(measure, recommendationList, transactionsCount, loadedMeasures);
            loadConfidenceIfRequired(measure, recommendationList, loadedMeasures);
            loadLiftIfRequired(measure, instanceName, recommendationList, transactionsCount, loadedMeasures);
        }
    }

    public ScoreFunctionResult<String> createScoreFunctionResult(Recommendation input, Set<Measure> loadedMeasures) {
        ScoreFunctionResult<String> result = new ScoreFunctionResult<>(input.getAssociationId());
        if(loadedMeasures.contains(Measure.COUNT)) {
            result.getMeasures().put(Measure.COUNT.name().toLowerCase(), input.getAssociationCount());
        }
        if(loadedMeasures.contains(Measure.SUPPORT)) {
            result.getMeasures().put(Measure.SUPPORT.name().toLowerCase(), input.getSupport());
        }
        if(loadedMeasures.contains(Measure.CONFIDENCE)) {
            result.getMeasures().put(Measure.CONFIDENCE.name().toLowerCase(), input.getConfidence());
        }
        if(loadedMeasures.contains(Measure.LIFT)) {
            result.getMeasures().put(Measure.LIFT.name().toLowerCase(), input.getLift());
        }
        return result;
    }

    @Override
    public long getTransactionsCount(String instanceName) throws IOException {
        return itemSetsDao.getItemSetCount(instanceName, TRANSACTIONS_COUNT_ROW_KEY);
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

    private void filterPhase(String instanceName, ScoreFunction<Recommendation> scoreFunction,
                             List<Recommendation> recommendationList, long transactionsCount, Set<Measure> loadedMeasures)
            throws IOException {
        for (Pair<Measure, Number> pair : scoreFunction.getThresholds()) {
            if (pair.getKey() == Measure.COUNT) {
                recommendationList.removeIf(recom -> recom.getAssociationCount() < pair.getValue().longValue());
            }
            if (loadSupportIfRequired(pair.getKey(), recommendationList, transactionsCount, loadedMeasures)) {
                recommendationList.removeIf(recom -> recom.getAssociationCount() < pair.getValue().longValue());
            }
            if (loadConfidenceIfRequired(pair.getKey(), recommendationList, loadedMeasures)) {
                recommendationList.removeIf(recom -> recom.getConfidence() < pair.getValue().doubleValue());
            }
        }
        for (Pair<Measure, Number> pair : scoreFunction.getThresholds()) {
            if (loadLiftIfRequired(pair.getKey(), instanceName, recommendationList, transactionsCount, loadedMeasures)) {
                recommendationList.removeIf(recom -> recom.getLift() < pair.getValue().doubleValue());
            }
        }
    }

    private void sortRecommendations(String instanceName, ScoreFunction<Recommendation> scoreFunction,
                                     List<Recommendation> recommendationList,
                                     long transactionsCount, Set<Measure> loadedMeasures) throws IOException {

        List<Comparator<Recommendation>> comparatorList = new ArrayList<>();

        for (Measure measure : scoreFunction.getSortParams()) {
            if (measure == Measure.COUNT) {
                comparatorList.add((o1, o2) -> -Long.compare(o1.getAssociationCount(), o2.getAssociationCount()));
            }
            if (loadSupportIfRequired(measure, recommendationList, transactionsCount, loadedMeasures)) {
                comparatorList.add((o1, o2) -> -Double.compare(o1.getSupport(), o2.getSupport()));
            }
            if (loadConfidenceIfRequired(measure, recommendationList, loadedMeasures)) {
                comparatorList.add((o1, o2) -> -Double.compare(o1.getConfidence(), o2.getConfidence()));
            }
            if(loadLiftIfRequired(measure, instanceName, recommendationList, transactionsCount, loadedMeasures)) {
                comparatorList.add((o1, o2) -> -Double.compare(o1.getLift(), o2.getLift()));
            }
        }
        Collections.sort(recommendationList, (o1, o2) -> {
            int result;
            for (Comparator<Recommendation> recommendationComparator : comparatorList) {
                result = recommendationComparator.compare(o1, o2);
                if(result != 0) return result;
            }
            return 0;
        });
    }

    private boolean loadSupportIfRequired(Measure measure, List<Recommendation> recommendationList,
                                          long transactionsCount, Set<Measure> loadedMeasures) {
        if (measure == Measure.SUPPORT) {
            if(loadedMeasures.contains(Measure.SUPPORT)) return true;
            recommendationList.forEach(recommendation -> calculateSupport(recommendation, transactionsCount));
            loadedMeasures.add(Measure.SUPPORT);
            return true;
        }
        return false;
    }

    private boolean loadConfidenceIfRequired(Measure measure, List<Recommendation> recommendationList,
                                             Set<Measure> loadedMeasures) {
        if (measure == Measure.CONFIDENCE) {
            if(loadedMeasures.contains(Measure.CONFIDENCE)) return true;
            recommendationList.forEach(this::calculateConfidence);
            loadedMeasures.add(Measure.CONFIDENCE);
            return true;
        }
        return false;
    }

    private boolean loadLiftIfRequired(Measure measure, String instanceName, List<Recommendation> recommendationList,
                                       long transactionsCount, Set<Measure> loadedMeasures) throws IOException {
        if (measure == Measure.LIFT) {
            if(loadedMeasures.contains(Measure.LIFT)) return true;
            calculateLift(instanceName, recommendationList, transactionsCount);
            loadedMeasures.add(Measure.LIFT);
            return true;
        }
        return false;
    }

    private void calculateSupport(Recommendation recommendation, long transactionsCount) {
        recommendation.setSupport((double) recommendation.getAssociationCount() / transactionsCount);
    }

    private void calculateConfidence(Recommendation recommendation) {
        recommendation.setConfidence((double) recommendation.getAssociationCount() / recommendation.getItemSetCount());
    }

    private void calculateLift(String instanceName, List<Recommendation> recommendationList, long transactionsCount)
            throws IOException {
        //TODO important place for performance
        Map<String, Long> assocCounts = itemSetsDao.getItemSetsCount(instanceName,
                new HashSet<>(Lists.transform(recommendationList, Recommendation::getAssociationId)));
        recommendationList.forEach(recommendation -> {
            Long count = assocCounts.get(recommendation.getAssociationId());
            if (count != null) recommendation.setCountOfAssociationAsItemSet(count);
        });

        for (Recommendation recommendation : recommendationList) {
            if(recommendation.getCountOfAssociationAsItemSet() == 0) continue;
            recommendation.setLift(recommendation.getAssociationCount() /
                    ((double)recommendation.getItemSetCount()
                            * recommendation.getCountOfAssociationAsItemSet()/transactionsCount));
            //divide by transactionCount
            //empty item-set => transactionCount
        }
    }

    private List<Recommendation> createBasicRecommendations(String instanceName, List<String> itemSets,
                                                            ScoreFunction<Recommendation> scoreFunction) throws IOException {
        List<Recommendation> recommendationList = new ArrayList<>();
        for (String itemSet : itemSets) {
            long itemSetCount = itemSetsDao.getItemSetCount(instanceName, itemSet);

            if(itemSetCount == 0) continue;

            Map<String, Long> associations;

            if(scoreFunction.containsThresholdsMeasure(Measure.COUNT)) {
                associations = itemSetsDao.getAssociations(instanceName, itemSet,
                        scoreFunction.getThresholdValue(Measure.COUNT).longValue());
            } else {
                associations = itemSetsDao.getAssociations(instanceName, itemSet);
            }

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

    @Override
    public Map<String, Long> getItemSetWithCountMore(String instanceName, long count) throws IOException {
        return itemSetsDao.getItemSetWithCountMore(instanceName, count);
    }
}
