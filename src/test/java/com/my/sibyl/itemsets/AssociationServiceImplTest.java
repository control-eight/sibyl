package com.my.sibyl.itemsets;

import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.score_function.BasicScoreFunction;
import com.my.sibyl.itemsets.score_function.ConfidenceRecommendationFilter;
import com.my.sibyl.itemsets.score_function.Recommendation;
import com.my.sibyl.itemsets.score_function.ScoreFunction;
import com.my.sibyl.itemsets.score_function.ScoreFunctionResult;
import com.my.sibyl.itemsets.util.ItemSetsGenerator;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

import static com.my.sibyl.itemsets.util.CombinationsGeneratorTest.createItemSetAndAssociation;
import static org.mockito.Mockito.*;

import static com.my.sibyl.itemsets.InstancesService.*;

/**
 * @author abykovsky
 * @since 1/25/15
 */
public class AssociationServiceImplTest {

    private ItemSetsDao mockItemSetsDao;

    private AssociationServiceImpl associationService;

    private ItemSetsGenerator mockItemSetsGenerator;

    @Before
    public void setUp() {
        mockItemSetsDao = mock(ItemSetsDao.class);
        mockItemSetsGenerator = mock(ItemSetsGenerator.class);
        associationService = new AssociationServiceImpl();
        associationService.setItemSetsDao(mockItemSetsDao);
        associationService.setItemSetsGenerator(mockItemSetsGenerator);
    }

    @Test
    public void testProcessTransaction() throws IOException, HBaseException {

        List<ItemSetAndAssociation<String>> collection = new ArrayList<>();
        collection.add(createItemSetAndAssociation("1", 1l, Arrays.asList("2", "3"), Arrays.asList(1l, 1l)));
        collection.add(createItemSetAndAssociation("2", 1l, Arrays.asList("1", "3"), Arrays.asList(1l, 1l)));
        collection.add(createItemSetAndAssociation("3", 1l, Arrays.asList("1", "2"), Arrays.asList(1l, 1l)));
        collection.add(createItemSetAndAssociation("1-2", 1l, Arrays.asList("3"), Arrays.asList(1l)));
        collection.add(createItemSetAndAssociation("1-3", 1l, Arrays.asList("2"), Arrays.asList(1l)));
        collection.add(createItemSetAndAssociation("2-3", 1l, Arrays.asList("1"), Arrays.asList(1l)));

        List<String> transactionItems = Arrays.asList("1", "2", "3");

        when(mockItemSetsGenerator.generateItemSetsAndAssociations(transactionItems, 1)).thenReturn(collection);
        associationService.addTransaction(DEFAULT, transactionItems);
        verify(mockItemSetsDao).incrementItemSetAndAssociations(DEFAULT, "1", 1l, collection.get(0).getAssociationMap());
        verify(mockItemSetsDao).incrementItemSetAndAssociations(DEFAULT, "2", 1l, collection.get(1).getAssociationMap());
        verify(mockItemSetsDao).incrementItemSetAndAssociations(DEFAULT, "3", 1l, collection.get(2).getAssociationMap());
        verify(mockItemSetsDao).incrementItemSetAndAssociations(DEFAULT, "1-2", 1l, collection.get(3).getAssociationMap());
        verify(mockItemSetsDao).incrementItemSetAndAssociations(DEFAULT, "1-3", 1l, collection.get(4).getAssociationMap());
        verify(mockItemSetsDao).incrementItemSetAndAssociations(DEFAULT, "2-3", 1l, collection.get(5).getAssociationMap());
    }

    @Test
    public void testGetRecommendations_Simple() throws IOException {

        List<String> basketItems = Arrays.asList("1", "2", "3");

        List<String> itemSets = Arrays.asList("1", "2", "3", "1-2", "1-3", "2-3");

        when(mockItemSetsGenerator.generateCombinations(basketItems)).thenReturn(itemSets);

        when(mockItemSetsDao.getItemSetCount(DEFAULT, "1")).thenReturn(10l);
        when(mockItemSetsDao.getItemSetCount(DEFAULT, "2")).thenReturn(5l);
        when(mockItemSetsDao.getItemSetCount(DEFAULT, "3")).thenReturn(10l);
        when(mockItemSetsDao.getItemSetCount(DEFAULT, "1-2")).thenReturn(1l);
        when(mockItemSetsDao.getItemSetCount(DEFAULT, "1-3")).thenReturn(10l);
        when(mockItemSetsDao.getItemSetCount(DEFAULT, "2-3")).thenReturn(10l);

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


        List<ScoreFunctionResult<String>> testResult = associationService.getRecommendations(InstancesService.DEFAULT,
                basketItems, scoreFunction);
        assertEquals("ScoreFunctionResult size", 3, testResult.size());
        assertEquals("Item id [0]", "1", testResult.get(0).getResult());
        assertEquals("Item id [1]", "2", testResult.get(1).getResult());
        assertEquals("Item id [2]", "3", testResult.get(2).getResult());
    }

    @Test
    public void testGetRecommendations_WithLift() throws IOException {

        List<String> basketItems = Arrays.asList("1", "2", "3");

        List<String> itemSets = Arrays.asList("1", "2", "3", "1-2", "1-3", "2-3");

        when(mockItemSetsGenerator.generateCombinations(basketItems)).thenReturn(itemSets);

        when(mockItemSetsDao.getItemSetCount(DEFAULT, "1")).thenReturn(10l);
        when(mockItemSetsDao.getItemSetCount(DEFAULT, "2")).thenReturn(5l);
        when(mockItemSetsDao.getItemSetCount(DEFAULT, "3")).thenReturn(10l);
        when(mockItemSetsDao.getItemSetCount(DEFAULT, "1-2")).thenReturn(1l);
        when(mockItemSetsDao.getItemSetCount(DEFAULT, "1-3")).thenReturn(10l);
        when(mockItemSetsDao.getItemSetCount(DEFAULT, "2-3")).thenReturn(10l);

        mockGetAssociations("1", Arrays.asList("2", "3"), Arrays.asList(1l, 10l));
        mockGetAssociations("2", Arrays.asList("1", "3"), Arrays.asList(1l, 10l));
        mockGetAssociations("3", Arrays.asList("1", "2"), Arrays.asList(10l, 10l));

        mockGetAssociations("1-2", Arrays.asList("3"), Arrays.asList(1l));
        mockGetAssociations("1-3", Arrays.asList("2"), Arrays.asList(4l));
        mockGetAssociations("2-3", Arrays.asList("1"), Arrays.asList(10l));

        Map<String, Long> map = new HashMap<>();
        map.put("1", 10l); map.put("2", 5l); map.put("3", 10l);
        Set<String> set = new HashSet<>();
        set.add("1"); set.add("2"); set.add("3");
        when(mockItemSetsDao.getItemSetsCount(DEFAULT, set)).thenReturn(map);

        when(mockItemSetsDao.getItemSetCount(DEFAULT, AssociationServiceImpl.TRANSACTIONS_COUNT_ROW_KEY)).thenReturn(1l);

        boolean isLiftInUse = true;
        double confidence = 0.5;
        int maxResults = 10;
        ScoreFunction<Recommendation> scoreFunction = new BasicScoreFunction(maxResults,
                Arrays.asList(new ConfidenceRecommendationFilter() {
                    @Override
                    public boolean filter(Double value) {
                        return value < confidence;
                    }
                }), isLiftInUse);


        List<ScoreFunctionResult<String>> testResult = associationService.getRecommendations(InstancesService.DEFAULT,
                basketItems, scoreFunction);
        assertEquals("ScoreFunctionResult size", 3, testResult.size());
        assertEquals("Item id [0]", "1", testResult.get(0).getResult());
        assertEquals("Item id [1]", "2", testResult.get(1).getResult());
        assertEquals("Item id [2]", "3", testResult.get(2).getResult());
        assertEquals("Lift 1", 0.1, testResult.get(0).getMeasures().get("lift"));
        assertEquals("Lift 2", 0.2, testResult.get(1).getMeasures().get("lift"));
        assertEquals("Lift 3", 0.2, testResult.get(2).getMeasures().get("lift"));
    }

    private Map<String, Long> mockGetAssociations(String itemSetRowKey, List<String> keys, List<Long> values) throws IOException {
        Map<String, Long> map = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            map.put(keys.get(i), values.get(i));
        }
        when(mockItemSetsDao.getAssociations(DEFAULT, itemSetRowKey)).thenReturn(map);
        return map;
    }
}
