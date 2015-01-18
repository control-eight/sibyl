package com.my.sibyl.itemsets.kiji;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author abykovsky
 * @since 12/22/14
 */
public class KijiCandidatesGeneratorOld {

    private static final KijiDataRequest RECOMMENDED_ITEMS_REQUEST = KijiDataRequest
            .create(ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.RECOMMENDATIONS.getName());

    private static final Double MIN_SUPPORT = 2.0;

    private static final Double MIN_CONFIDENCE = 0.05;

    private static final Double MIN_LIFT = 1.0;

    private final KijiTable kijiTable;

    private final AtomicKijiPutter kijiPutter;

    private final KijiTableReader kijiTableReader;

    private final KijiFrequentItemSetsGenerator frequentItemSetsGenerator;

    public KijiCandidatesGeneratorOld(final KijiTable kijiTable, final AtomicKijiPutter kijiPutter,
                                      final KijiTableReader kijiTableReader,
                                      final KijiFrequentItemSetsGenerator frequentItemSetsGenerator) {
        this.kijiTable = kijiTable;
        this.kijiPutter = kijiPutter;
        this.kijiTableReader = kijiTableReader;
        this.frequentItemSetsGenerator = frequentItemSetsGenerator;
    }

    //left in pair is old, right is new frequency
    public void process(Map<Set<Long>, Pair<Integer, Integer>> itemSetsDifference, int transactionsCount) throws IOException {
        this.innerProcess(itemSetsDifference, itemSetsDifference, transactionsCount);
    }

    /**
     * We should either:
     * 1. Add new recommendations and add new recommended item
     * 2. Add new recommended item and update others
     * 3. Update recommended item and update others
     *
     * So we either:
     * 1. Add new row & column
     * 2. Update column
     *
     * We've got only put operations so we don't have to make difference between insert & update.
     *
     * We make either add new row or gathering items for update
     *
     * to update confidence or lift we could use following approach
     * (a/b) * x = (a/(b + delta)); x = (a*b/a*(b+delta)) = b/(b + delta)
     * so we just need to know old lfsFreq and new lfsFreq
     */
    //gather structure
    //symmetric values
    /**
     * Set<Long> - lfs
     * Pair<RecommendedProducts, RecommendedProducts>
     *  left - old (compare when commit)
     *  left - new
     * Map<Set<Long>, Integer>> - input data to repeat process if commit was unsuccessful
     */
    private void innerProcess(Map<Set<Long>, Pair<Integer, Integer>> itemSetsDifference, Map<Set<Long>, Pair<Integer, Integer>> lfsMap,
                        int transactionsCount) throws IOException {

        Map<Set<Long>, Pair<Pair<RecommendedProducts, RecommendedProducts>,
                Map<Set<Long>, Pair<Integer, Integer>>>> recommendedProductsForChangeMap = new HashMap<>();

        //gather phase
        for (Map.Entry<Set<Long>, Pair<Integer, Integer>> entry : itemSetsDifference.entrySet()) {
            if (!checkItemSetSize(entry)) continue;
            int itemSetFreq = entry.getValue().getRight();

            if (!checkSupport(itemSetFreq)) continue;

            Set<Long> itemSet = entry.getKey();

            //we only consider to build k-1 candidates in order to optimize code
            int i = 0;
            for(Iterator<Long> itemSetIter = itemSet.iterator(); itemSetIter.hasNext();) {
                Set<Long> lfs = generateCandidates(itemSet, i++);
                Long rhs = itemSetIter.next();

                double confidence = calcConfidenceValue(itemSetFreq, lfs);
                double lift = calculateLift(confidence, rhs, transactionsCount);

                if (!checkConfidenceAndLift(confidence, lift)) continue;

                createRecommendations(recommendedProductsForChangeMap, entry, itemSetFreq, lfs, rhs, lift);
            }
        }
        updateRecommendations(lfsMap, recommendedProductsForChangeMap);
        commitRecommendations(lfsMap, transactionsCount, recommendedProductsForChangeMap, null);
    }

    /**
     * There are main differences:
     * 1. We want to check support, confidence, lift to make decision: update or delete
     * 2. There should no case when particular row or column doesn't exist
     * so we could skip these cases and process only case 3
     * 3.
     */
    public void processRemoving(Map<Set<Long>, Pair<Integer, Integer>> itemSetsDifference, int transactionsCount) throws IOException {
        this.innerProcessRemoving(itemSetsDifference, itemSetsDifference, transactionsCount);
    }

    private void innerProcessRemoving(Map<Set<Long>, Pair<Integer, Integer>> itemSetsDifference,
                                      Map<Set<Long>, Pair<Integer, Integer>> lfsMap,
                                      int transactionsCount) throws IOException {
        Map<Set<Long>, Pair<Pair<RecommendedProducts, RecommendedProducts>,
                Map<Set<Long>, Pair<Integer, Integer>>>> recommendedProductsForChangeMap = new HashMap<>();

        //gather phase
        Multimap<Set<Long>, Long> toRemoveMap = HashMultimap.create();
        for (Map.Entry<Set<Long>, Pair<Integer, Integer>> entry : itemSetsDifference.entrySet()) {
            if (!checkItemSetSize(entry)) continue;
            int itemSetFreq = entry.getValue().getRight();

            boolean itemSetFreqToRemove = !checkSupport(itemSetFreq);

            Set<Long> itemSet = entry.getKey();

            //we only consider to build k-1 candidates in order to optimize code
            int i = 0;
            for(Iterator<Long> itemSetIter = itemSet.iterator(); itemSetIter.hasNext();) {
                Set<Long> lfs = generateCandidates(itemSet, i++);
                Long rhs = itemSetIter.next();

                double confidence = calcConfidenceValue(itemSetFreq, lfs);
                double lift = calculateLift(confidence, rhs, transactionsCount);

                boolean recommendationToRemove = itemSetFreqToRemove;
                recommendationToRemove |= !checkConfidenceAndLift(confidence, lift);

                createRecommendationsForRemoving(recommendedProductsForChangeMap, itemSetFreq, lfs, rhs, lift,
                        recommendationToRemove, toRemoveMap);
            }
        }
        updateRecommendations(lfsMap, recommendedProductsForChangeMap);
        commitRecommendations(lfsMap, transactionsCount, recommendedProductsForChangeMap, toRemoveMap);
    }

    private boolean checkItemSetSize(Map.Entry<Set<Long>, Pair<Integer, Integer>> entry) {
        return entry.getKey().size() > 1;
    }

    private boolean checkConfidenceAndLift(double confidence, double lift) {
        return confidence >= MIN_CONFIDENCE && lift >= MIN_LIFT;
    }

    private boolean checkSupport(int itemSetFreq) {
        return calcSupportValue(itemSetFreq) >= MIN_SUPPORT;
    }

    //in memory
    //update lift based on new lfsFreq value
    private void updateRecommendations(Map<Set<Long>, Pair<Integer, Integer>> lfsMap, Map<Set<Long>,
            Pair<Pair<RecommendedProducts, RecommendedProducts>, Map<Set<Long>, Pair<Integer, Integer>>>> recommendedProductsForChangeMap) {
        for (Map.Entry<Set<Long>, Pair<Pair<RecommendedProducts, RecommendedProducts>, Map<Set<Long>, Pair<Integer, Integer>>>> entry :
                recommendedProductsForChangeMap.entrySet()) {
            RecommendedProducts oldP = entry.getValue().getLeft().getLeft();
            RecommendedProducts newP = entry.getValue().getLeft().getRight();

            for (int i = 0; i < newP.getProducts().size(); i++) {
                RecommendedProduct recommendedProduct = newP.getProducts().get(i);
                boolean isNew = true;
                for (RecommendedProduct product : oldP.getProducts()) {
                    if(recommendedProduct == product) {
                        isNew = false;
                        break;
                    }
                }
                if(!isNew) {
                    RecommendedProduct newRecommendedProduct = copyRecommendedProduct(recommendedProduct);
                    //(a/b) * x = (a/(b + delta)); x = (a*b/a*(b+delta)) = b/(b + delta)
                    newRecommendedProduct.getScore().setLift(newRecommendedProduct.getScore().getLift() *
                            (lfsMap.get(entry.getKey()).getLeft())
                            / (lfsMap.get(entry.getKey()).getRight()));
                    newP.getProducts().set(i, newRecommendedProduct);
                }
            }
        }
    }

    //to data store
    //commit phase. optimistic commit
    private void commitRecommendations(Map<Set<Long>, Pair<Integer, Integer>> lfsMap, int transactionsCount, Map<Set<Long>,
            Pair<Pair<RecommendedProducts, RecommendedProducts>, Map<Set<Long>, Pair<Integer, Integer>>>> recommendedProductsForChangeMap,
                                       Multimap<Set<Long>, Long> toRemoveMap)
            throws IOException {
        for (Map.Entry<Set<Long>, Pair<Pair<RecommendedProducts, RecommendedProducts>, Map<Set<Long>, Pair<Integer, Integer>>>> entry :
                recommendedProductsForChangeMap.entrySet()) {

            Pair<RecommendedProducts, RecommendedProducts> pair = entry.getValue().getLeft();

            boolean putResult = this.putRecommendation(entry.getKey(), pair.getLeft(), pair.getRight());
            //System.out.println("Result of putting " + pair.getRight() + " is " + putResult + " old product is " + pair.getLeft());
            if(!putResult) {
                //another repeat (now map of item sets is smaller is bound by only one lfs)
                //TODO TODO TODO
                //!!!!!!!!!!!!!!
                //OPTIMIZATION IS REQUIRED
                //We should only repeat process for only single lfs not for all!!!
                //this.innerProcess(entry.getValue().getRight(), lfsMap, transactionsCount);
                //this.reprocessRecommendation(entry.getKey(), pair.getRight());
                reCreateRecommendations(entry.getKey(), pair.getRight());
            }
        }
    }

    private void reCreateRecommendations(Set<Long> lfs, RecommendedProducts newRecommendedProducts) throws IOException {
        RecommendedProducts oldRecommendedProducts = getRecommendedProducts(lfs);

        //recommendations were deleted by sliding window process
        //cancel update
        if(oldRecommendedProducts == null) return;

        for(Iterator<RecommendedProduct> iter = newRecommendedProducts.getProducts().iterator(); iter.hasNext();) {
            boolean found = false;
            RecommendedProduct newRecommendedProduct = iter.next();
            for (RecommendedProduct oldRecommendedProduct : oldRecommendedProducts.getProducts()) {
                if(oldRecommendedProduct.getId().equals(newRecommendedProduct.getId())) {
                    found = true;
                    //our value isn't most recent take exists one
                    if(newRecommendedProduct.getScore().getFrequency()
                            < oldRecommendedProduct.getScore().getFrequency()) {
                        newRecommendedProduct.setScore(oldRecommendedProduct.getScore());
                    }
                    break;
                }
            }
            //we only look on exists products in oldRecommendedProduct
            //cause if product was removed we should cancel update
            if(!found) {
                iter.remove();
            }
        }

        boolean putResult = this.putRecommendation(lfs, oldRecommendedProducts, newRecommendedProducts);
        if(!putResult) {
            this.reCreateRecommendations(lfs, newRecommendedProducts);
        }
    }

    private void createRecommendations(Map<Set<Long>, Pair<Pair<RecommendedProducts, RecommendedProducts>, Map<Set<Long>,
            Pair<Integer, Integer>>>> recommendedProductsForChangeMap, Map.Entry<Set<Long>, Pair<Integer, Integer>> entry,
                                       int itemSetFreq, Set<Long> lfs, Long rhs, double lift) throws IOException {
        RecommendedProducts recommendedProducts = getRecommendedProducts(lfs);

        //case 1. Add new recommendations and add new recommended item
        if(recommendedProducts == null) {
            addRecommendation(lfs, rhs, createScore(itemSetFreq, lift));
        } else {
            RecommendedProduct foundRecommendedProduct = null;
            for (RecommendedProduct recommendedProduct : recommendedProducts.getProducts()) {
                if(rhs.equals(recommendedProduct.getId())) {
                    foundRecommendedProduct = recommendedProduct;
                    break;
                }
            }

            RecommendedProducts newRecommendedProducts = new RecommendedProducts(
                    new ArrayList<>(recommendedProducts.getProducts())
            );

            recommendedProductsForChangeMap.putIfAbsent(lfs,
                    new ImmutablePair<>(new ImmutablePair<RecommendedProducts,
                            RecommendedProducts>(recommendedProducts, newRecommendedProducts),
                            new HashMap<>()));
            recommendedProductsForChangeMap.get(lfs).getRight().putIfAbsent(entry.getKey(),
                    entry.getValue());

            //case 2. Add new recommended item and update others
            if(foundRecommendedProduct == null) {
                recommendedProductsForChangeMap.get(lfs).getLeft().getRight().getProducts()
                        .add(new RecommendedProduct(rhs, createScore(itemSetFreq, lift)));
                //case 3. Update recommended item and update others
            } else {
                updateRecommendedItemAndUpdateOthers(recommendedProductsForChangeMap, itemSetFreq, lfs, rhs, lift, false,
                        null);
            }
        }
    }

    private void updateRecommendedItemAndUpdateOthers(Map<Set<Long>, Pair<Pair<RecommendedProducts, RecommendedProducts>,
                                                      Map<Set<Long>, Pair<Integer, Integer>>>> recommendedProductsForChangeMap,
                                                      int itemSetFreq, Set<Long> lfs, Long rhs, double lift, boolean toRemove,
                                                      Multimap<Set<Long>, Long> toRemoveMap) {
        for(int j = 0; j < recommendedProductsForChangeMap.get(lfs).getLeft().getRight()
                .getProducts().size(); j++) {
            RecommendedProduct recommendedProduct = recommendedProductsForChangeMap.get(lfs)
                    .getLeft().getRight().getProducts().get(j);
            if(rhs.equals(recommendedProduct.getId())) {
                RecommendedProduct newRecommendedProduct = copyRecommendedProduct(recommendedProduct);
                newRecommendedProduct.getScore().setFrequency(itemSetFreq);
                newRecommendedProduct.getScore().setLift(lift);
                recommendedProductsForChangeMap.get(lfs)
                        .getLeft().getRight().getProducts().set(j, newRecommendedProduct);
                if(toRemove) {
                    toRemoveMap.put(lfs, newRecommendedProduct.getId());
                }
                break;
            }
        }
    }

    private RecommendedProduct copyRecommendedProduct(RecommendedProduct recommendedProduct) {
        RecommendedProduct result = new RecommendedProduct(recommendedProduct.getId(),
                new Score(recommendedProduct.getScore().getFrequency(),
                        recommendedProduct.getScore().getLift(),
                        recommendedProduct.getScore().getSuccessFrequency()));
        return result;
    }

    /**
     * lift = confidence/supportrhs
     * lift = (entireFreq * txCount) / (lfsFreq * rfsFreq)
     */
    private double calculateLift(double confidence, Long rhs, int transactionCount) throws IOException {
        double result = confidence/((double)frequentItemSetsGenerator.getCount(rhs)/transactionCount);
        //System.out.println("Lift for: " + rhs + " is " + result);
        return result;
    }

    /**
     * confidence = entireFreq / lfsFreq
     */
    private double calcConfidenceValue(Integer entireFreq, Set<Long> lfs) throws IOException {
        Integer lfsFreq = frequentItemSetsGenerator.getCount(lfs);
        if(lfsFreq == 0) return 0;
        if(entireFreq > lfsFreq) {
            System.out.println("CONFIDENCE: " + entireFreq + " " + lfsFreq + " " + lfs);
        }
        return ((double)entireFreq/lfsFreq);
    }

    private Score createScore(Integer itemSetFreq, double lift) {
        Score result = new Score();
        result.setFrequency(itemSetFreq);
        result.setLift(lift);
        return result;
    }

    public RecommendedProducts getRecommendedProducts(Set<Long> itemSet) throws IOException {
        final EntityId itemSetId = kijiTable.getEntityId(frequentItemSetsGenerator.format(itemSet));

        return kijiTableReader.get(itemSetId, RECOMMENDED_ITEMS_REQUEST)
                .getMostRecentValue(ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.RECOMMENDATIONS.getName());
    }

    private void addRecommendation(Set<Long> itemSet, Long recommendationItem, Score score) throws IOException {
        final EntityId itemSetId = kijiTable.getEntityId(frequentItemSetsGenerator.format(itemSet));
        kijiPutter.begin(itemSetId);
        final long timestamp = System.currentTimeMillis();

        RecommendedProducts recommendedProducts = new RecommendedProducts(Arrays
                .asList(new RecommendedProduct(recommendationItem, score)));

        kijiPutter.put(ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.RECOMMENDATIONS.getName(), timestamp, recommendedProducts);
        kijiPutter.checkAndCommit(ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.RECOMMENDATIONS.getName(), null);
        //we don't to repeat in failure commit case cause it means someone has already added count
        //TODO !!!but we have to!!!
    }

    private boolean putRecommendation(Set<Long> itemSet, RecommendedProducts oldRecommendedProducts,
                                   RecommendedProducts newRecommendedProducts) throws IOException {
        final EntityId itemSetId = kijiTable.getEntityId(frequentItemSetsGenerator.format(itemSet));
        kijiPutter.begin(itemSetId);
        final long timestamp = System.currentTimeMillis();

        kijiPutter.put(ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.RECOMMENDATIONS.getName(), timestamp, newRecommendedProducts);
        boolean result = kijiPutter.checkAndCommit(ItemSetsFields.INFO_FAMILY.getName(), ItemSetsFields.RECOMMENDATIONS.getName(), oldRecommendedProducts);
        if(!result) {
            kijiPutter.rollback();
        }
        return result;
    }

    /*public void process(Map<Set<Long>, Long> successfulCandidates) {
        for (Map.Entry<Set<Long>, Long> entry : successfulCandidates.entrySet()) {
            Map<Long, Score> map = candidates.get(entry.getKey());
            if(map != null) {
                Score score = map.get(entry.getValue());
                if(score != null) {
                    score.setSuccessFrequency(score.getSuccessFrequency() + 1);
                    scoreFunction(score);
                }
            }
        }
    }*/

    private Set<Long> generateCandidates(Set<Long> itemSet, int i) {
        Set<Long> newItemSet = new HashSet<>();
        int j = 0;
        for (Iterator<Long> innerItemSetIter = itemSet.iterator(); innerItemSetIter.hasNext();) {
            if(j != i) newItemSet.add(innerItemSetIter.next());
            else innerItemSetIter.next();
            j++;
        }
        return newItemSet;
    }

    private double calcSupportValue(Integer freq) {
        //return ((double)freq/transactionsCount);
        return freq;
    }

    private void createRecommendationsForRemoving(Map<Set<Long>, Pair<Pair<RecommendedProducts, RecommendedProducts>, Map<Set<Long>,
            Pair<Integer, Integer>>>> recommendedProductsForChangeMap,
            int itemSetFreq, Set<Long> lfs, Long rhs, double lift, boolean toRemove, Multimap<Set<Long>, Long> toRemoveMap) throws IOException {
        RecommendedProducts recommendedProducts = getRecommendedProducts(lfs);
        if(recommendedProducts == null) return;
        updateRecommendedItemAndUpdateOthers(recommendedProductsForChangeMap, itemSetFreq, lfs, rhs, lift, toRemove, toRemoveMap);
    }

    /**
     * Simply summarize both values
     * @return
     */
    /*private Score scoreFunction(Score score) {
        score.setScore(score.getFrequency() + score.getSuccessFrequency());
        return score;
    }*/

    public void print() {
        //System.out.println("MinFreq: " + minFreq + " MaxFreq: " + maxFreq);
        //System.out.println("Size: " + candidates.size() + ". Candidates: " + candidates);
        /*int i = 0;
        for (Map.Entry<Set<Long>, Map<Long, Score>> entry : candidates.entrySet()) {
            System.out.println(entry);
            if(++i == 1000) break;
        }*/
    }

    //TODO: get recommendations by using permutation of basketItems
    /*public List<Container> getTopRecommendations(Set<Long> basketItems, int maxCount, int transactionsCount) {
        List<Container> list = new ArrayList<>();

        Map<Long, Score> map = candidates.get(basketItems);
        if(map == null) return Collections.emptyList();

        for (Iterator<Map.Entry<Long, Score>> iter = map.entrySet().iterator(); iter.hasNext();) {

            Map.Entry<Long, Score> entry = iter.next();

            Set<Long> allItemList = new HashSet<>(basketItems);
            allItemList.add(entry.getKey());
            Integer freq = frequentItemSetsGenerator.getCount(allItemList);

            entry.getValue().setFrequency(freq);
            scoreFunction(entry.getValue());

            *//*double supportValue = calcSupportValue(transactionsCount, freq);
            if(supportValue < MIN_SUPPORT) {
                //System.out.println("New support value: " + supportValue + " for " + entry.getKey() + " " + allItemList);
                iter.remove();
                continue;
            }*//*

            double confidence = (double) freq / frequentItemSetsGenerator.getCount(basketItems);
            *//*if(confidence < MIN_CONFIDENCE) {
                //System.out.println("New confidence: " + confidence + " for " + entry.getKey());
                iter.remove();
                continue;
            } else {
                entry.getValue().setConfidence(confidence);
            }*//*
            entry.getValue().setConfidence(confidence);

            list.add(new Container(entry.getKey(), new GlobalScore(entry.getValue().getScore(), confidence)));
        }

        if(candidates.get(basketItems).isEmpty()) {
            candidates.remove(basketItems);
        }

        List<Container> result = new ArrayList<Container>(Ordering.natural().greatestOf(list, maxCount));
        return result;
        return null;
    }

    public void update(int maxCount, int transactionsCount) {
        Map<Set<Long>, Map<Long, Score>> newC = new HashMap<>(candidates);
        for (Map.Entry<Set<Long>, Map<Long, Score>> entry : newC.entrySet()) {
            getTopRecommendations(entry.getKey(), maxCount, transactionsCount);
        }
    }*/
}
