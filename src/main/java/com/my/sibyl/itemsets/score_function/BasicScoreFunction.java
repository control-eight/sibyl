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
    }

    @Override
    public List<RecommendationFilter> getRecommendationFilters() {
        return recommendationFilters;
    }

    @Override
    public void calculateScore(Recommendation recommendation) {
        recommendation.setScore(recommendation.getLift() * 1e6 + recommendation.getAssociationCount());
    }

    @Override
    public int compare(Recommendation o1, Recommendation o2) {
        double result = o1.getScore() - o2.getScore();
        if(result < 0) return 1;
        if(result > 0) return -1;
        return 0;
    }

    /*@Override
    public int compare(Recommendation o1, Recommendation o2) {
        double result = o1.getLift() - o2.getLift();
        if(result < 0) return 1;
        if(result > 0) return -1;

        return (int) (-1 * (o1.getCount() - o2.getCount()));
    }*/

    @Override
    public int getMaxResults() {
        return maxResults;
    }

    @Override
    public boolean isLiftInUse() {
        return isLiftInUse;
    }
}
