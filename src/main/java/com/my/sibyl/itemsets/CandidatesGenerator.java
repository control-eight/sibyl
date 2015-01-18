package com.my.sibyl.itemsets;

import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TODO: implement optimized distributed selection algorithm
 * @author abykovsky
 * @since 11/24/14
 */
public class CandidatesGenerator {

    private final Double minSupport;

    private final Double minConfidence;

    private final Map<Set<Long>, Map<Long, Score>> candidates = new HashMap<>();

    private final FrequentItemSetsGenerator frequentItemSetsGenerator;

    private final ItemSetsFilter itemSetsFilter;

    public CandidatesGenerator(Double minSupport, Double minConfidence, FrequentItemSetsGenerator frequentItemSetsGenerator,
                               ItemSetsFilter itemSetsFilter) {
        this.minSupport = minSupport;
        this.minConfidence = minConfidence;
        this.frequentItemSetsGenerator = frequentItemSetsGenerator;
        this.itemSetsFilter = itemSetsFilter;
    }

    public void process(Map<Set<Long>, Integer> itemSetsDifference, int transactionsCount) {
        for (Map.Entry<Set<Long>, Integer> entry : itemSetsDifference.entrySet()) {
            if(entry.getKey().size() == 1) continue;
            if(calcSupportValue(transactionsCount, entry.getValue()) > minSupport) {
                Set<Long> itemSet = entry.getKey();

                //we only consider to build k-1 candidates in order to optimize code
                int i = 0;
                for(Iterator<Long> itemSetIter = itemSet.iterator(); itemSetIter.hasNext();) {
                    Set<Long> newItemSet = generateCandidates(itemSet, i++);
                    Long recommendationItem = itemSetIter.next();

                    double confidence = calcConfidenceValue(entry.getValue(), itemSet, newItemSet);
                    if(confidence > minConfidence) {
                        Integer itemSetFreq = entry.getValue();
                        if(!candidates.containsKey(newItemSet)) {
                            Map<Long, Score> map = new HashMap<>();
                            map.put(recommendationItem, scoreFunction(new Score(itemSetFreq, confidence)));
                            candidates.put(newItemSet, map);
                        } else {
                            if(!candidates.get(newItemSet).containsKey(recommendationItem)) {
                                candidates.get(newItemSet).put(recommendationItem, scoreFunction(new Score(itemSetFreq, confidence)));
                            } else {
                                Score score = candidates.get(newItemSet).get(recommendationItem);
                                score.setFrequency(itemSetFreq);
                                score.setConfidence(confidence);
                                scoreFunction(score);
                            }
                        }
                    }
                }
            }
        }
    }

    public void process(Map<Set<Long>, Long> successfulCandidates) {
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
    }

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

    private double calcConfidenceValue(Integer freq, Set<Long> itemSet, Set<Long> newItemSet) {
        Integer count2 = frequentItemSetsGenerator.getCount(itemSet);
        Integer count = frequentItemSetsGenerator.getCount(newItemSet);
        if(count == 0) return 0;
        if(freq > count) {
            System.out.println("CONFIDENCE: " + freq + " " + count + " " + newItemSet + " basic: "
                    + itemSet + " count2: " + count2);
        }
        return ((double)freq/count);
    }

    private double calcSupportValue(Integer transactionsCount, Integer freq) {
        if(freq > transactionsCount) {
            System.out.println("SUPPORT: " + freq + " " + transactionsCount);
        }
        //return ((double)freq/transactionsCount);
        return freq;
    }

    /**
     * Simply summarize both values
     * @param score
     * @return
     */
    private Score scoreFunction(Score score) {
        score.setScore(score.getFrequency() + score.getSuccessFrequency());
        return score;
    }

    public void print() {
        //System.out.println("MinFreq: " + minFreq + " MaxFreq: " + maxFreq);
        System.out.println("Size: " + candidates.size() + ". Candidates: " + candidates);
        /*int i = 0;
        for (Map.Entry<Set<Long>, Map<Long, Score>> entry : candidates.entrySet()) {
            System.out.println(entry);
            if(++i == 1000) break;
        }*/
    }

    private static int minFreq = Integer.MAX_VALUE;

    private static int maxFreq = Integer.MIN_VALUE;

    //TODO: get recommendations by using permutation of basketItems
    public List<Container> getTopRecommendations(Set<Long> basketItems, int maxCount, int transactionsCount) {
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

            double supportValue = calcSupportValue(transactionsCount, freq);
            if(supportValue < minSupport) {
                //System.out.println("New support value: " + supportValue + " for " + entry.getKey() + " " + allItemList);
                iter.remove();
                continue;
            }

            double confidence = (double) freq / frequentItemSetsGenerator.getCount(basketItems);
            if(confidence < minConfidence) {
                //System.out.println("New confidence: " + confidence + " for " + entry.getKey());
                iter.remove();
                continue;
            } else {
                entry.getValue().setConfidence(confidence);
            }

            /*if(entry.getValue().getFrequency() > maxFreq) {
                maxFreq = entry.getValue().getFrequency();
            }

            if(entry.getValue().getFrequency() < minFreq) {
                minFreq = entry.getValue().getFrequency();
            }

            if(entry.getValue().getFrequency() == 509) {
                System.out.println("//" + basketItems + " " + map);
            }*/

            list.add(new Container(entry.getKey(), new GlobalScore(entry.getValue().getScore(), confidence)));
        }

        if(candidates.get(basketItems).isEmpty()) {
            candidates.remove(basketItems);
        }

        List<Container> result = new ArrayList<Container>(Ordering.natural().greatestOf(list, maxCount));
        return result;
    }

    public void update(int maxCount, int transactionsCount) {
        Map<Set<Long>, Map<Long, Score>> newC = new HashMap<>(candidates);
        for (Map.Entry<Set<Long>, Map<Long, Score>> entry : newC.entrySet()) {
            getTopRecommendations(entry.getKey(), maxCount, transactionsCount);
        }
    }
}
