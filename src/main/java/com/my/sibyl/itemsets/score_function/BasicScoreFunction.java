package com.my.sibyl.itemsets.score_function;

import java.util.ArrayList;
import java.util.List;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public class BasicScoreFunction implements ScoreFunction<Recommendation> {

    private int maxResults;

    private List<RecommendationFilter> recommendationFilters = new ArrayList<>();

    private boolean isLiftInUse;

    public BasicScoreFunction(int maxResults, List<RecommendationFilter> recommendationFilters, boolean isLiftInUse) {
        this.maxResults = maxResults;
        this.recommendationFilters = recommendationFilters;
        this.isLiftInUse = isLiftInUse;
    }

    @Override
    public List<RecommendationFilter> getRecommendationFilters() {
        return recommendationFilters;
    }

    @Override
    public List<Recommendation> cut(List<Recommendation> recommendationList) {
        return recommendationList.subList(0, Math.min(recommendationList.size(), maxResults));
    }

    @Override
    public int compare(Recommendation o1, Recommendation o2) {
        /*double result = o1.getConfidence() - o2.getConfidence();
        if(result < 0) return 1;
        if(result > 0) return -1;*/

        double result = o1.getLift() - o2.getLift();
        if(result < 0) return 1;
        if(result > 0) return -1;

        return (int) (-1 * (o1.getAssociationCount() - o2.getAssociationCount()));
    }

    @Override
    public boolean isLiftInUse() {
        return isLiftInUse;
    }

    @Override
    public String toString() {
        return "BasicScoreFunction{" +
                "maxResults=" + maxResults +
                ", recommendationFilters=" + recommendationFilters +
                ", isLiftInUse=" + isLiftInUse +
                '}';
    }
}
