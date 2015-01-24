package com.my.sibyl.itemsets;

import com.google.common.collect.Sets;
import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.score_function.BasicScoreFunction;
import com.my.sibyl.itemsets.score_function.ConfidenceRecommendationFilter;
import com.my.sibyl.itemsets.score_function.Recommendation;
import com.my.sibyl.itemsets.score_function.ScoreFunction;
import com.my.sibyl.itemsets.score_function.ScoreFunctionResult;
import com.my.sibyl.itemsets.util.CombinationsGenerator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;

/**
 * @author abykovsky
 * @since 1/25/15
 */
public class AssociationServiceImplTest {

    private ItemSetsDao mockItemSetsDao;

    private AssociationServiceImpl associationService;

    private CombinationsGenerator<String> mockCombinationsGenerator;

    @Before
    public void setUp() {
        mockItemSetsDao = mock(ItemSetsDao.class);
        mockCombinationsGenerator = mock(CombinationsGenerator.class);
        associationService = new AssociationServiceImpl();
        associationService.setItemSetsDao(mockItemSetsDao);
        associationService.setCombinationsGenerator(mockCombinationsGenerator);
    }

    @Test
    public void testGetRecommendations_Simple() throws IOException {

        List<String> basketItems = Arrays.asList("1", "2", "3");

        Collection<Set<String>> itemSets = Arrays.asList(
                Sets.newHashSet("1"),
                Sets.newHashSet("2"),
                Sets.newHashSet("3"),
                Sets.newHashSet("1-2"),
                Sets.newHashSet("1-3"),
                Sets.newHashSet("2-3")
        );

        when(mockCombinationsGenerator.generateItemSets(basketItems)).thenReturn(itemSets);

        when(mockItemSetsDao.getCount("1")).thenReturn(10l);
        when(mockItemSetsDao.getCount("2")).thenReturn(5l);
        when(mockItemSetsDao.getCount("3")).thenReturn(10l);
        when(mockItemSetsDao.getCount("1-2")).thenReturn(1l);
        when(mockItemSetsDao.getCount("1-3")).thenReturn(10l);
        when(mockItemSetsDao.getCount("2-3")).thenReturn(10l);

        mockGetAssociations("1", Arrays.asList("2", "3"), Arrays.asList(1l, 10l));
        mockGetAssociations("2", Arrays.asList("1", "3"), Arrays.asList(1l, 10l));
        mockGetAssociations("3", Arrays.asList("2", "3"), Arrays.asList(10l, 10l));

        mockGetAssociations("1-2", Arrays.asList("3"), Arrays.asList(1l));
        mockGetAssociations("1-3", Arrays.asList("2"), Arrays.asList(4l));
        mockGetAssociations("2-3", Arrays.asList("1"), Arrays.asList(10l));

        boolean isLiftInUse = false;
        double confidence = 0.5;
        int maxResults = 10;
        ScoreFunction<Recommendation> scoreFunction = new BasicScoreFunction(maxResults,
                Arrays.asList(new ConfidenceRecommendationFilter() {
            @Override
            public boolean filter(Double value) {
                return value < confidence;
            }
        }), isLiftInUse);


        List<ScoreFunctionResult<String, Double>> testResult = associationService.getRecommendations(basketItems, scoreFunction);
        assertEquals("ScoreFunctionResult size", 3, testResult.size());
        assertEquals("Item id [0]", "1", testResult.get(0).getResult());
        assertEquals("Item id [1]", "2", testResult.get(1).getResult());
        assertEquals("Item id [2]", "3", testResult.get(2).getResult());
    }

    private Map<String, Long> mockGetAssociations(String itemSetRowKey, List<String> keys, List<Long> values) throws IOException {
        Map<String, Long> map = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            map.put(keys.get(i), values.get(i));
        }
        when(mockItemSetsDao.getAssociations(itemSetRowKey)).thenReturn(map);
        return map;
    }
}
