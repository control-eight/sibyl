package com.my.sibyl.itemsets.score_function;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public class BaseScoreFunctionTest {

    @Test
    public void testCompare() {
        List<Recommendation> list = Arrays.asList(createRecommendation(9.0, 5), createRecommendation(10.0, 15),
                createRecommendation(8.0, 5), createRecommendation(8.0, 10), createRecommendation(10.0, 20));

        ScoreFunction<Recommendation> scoreFunction = new BasicScoreFunction(-1, Collections.emptyList(), true);
        list.forEach(scoreFunction::calculateScore);
        Collections.sort(list, scoreFunction);

        double delta = 1e-10;
        assertEquals("Lift [0]", 10.0, list.get(0).getLift(), delta);
        assertEquals("Count [0]", 20, list.get(0).getAssociationCount());

        assertEquals("Lift [1]", 10.0, list.get(1).getLift(), delta);
        assertEquals("Count [1]", 15, list.get(1).getAssociationCount());

        assertEquals("Lift [2]", 9.0, list.get(2).getLift(), delta);
        assertEquals("Count [2]", 5, list.get(2).getAssociationCount());

        assertEquals("Lift [3]", 8.0, list.get(3).getLift(), delta);
        assertEquals("Count [3]", 10, list.get(3).getAssociationCount());

        assertEquals("Lift [4]", 8.0, list.get(4).getLift(), delta);
        assertEquals("Count [4]", 5, list.get(4).getAssociationCount());
    }

    private Recommendation createRecommendation(double lift, int count) {
        Recommendation recommendation = new Recommendation();
        recommendation.setLift(lift);
        recommendation.setAssociationCount(count);
        return recommendation;
    }
}
