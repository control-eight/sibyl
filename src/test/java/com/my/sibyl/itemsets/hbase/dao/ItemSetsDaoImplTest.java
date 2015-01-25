package com.my.sibyl.itemsets.hbase.dao;

import com.my.sibyl.itemsets.score_function.Recommendation;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;

import static com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl.*;

/**
 * @author abykovsky
 * @since 1/25/15
 */
public class ItemSetsDaoImplTest {

    private HConnection mockConnection;

    private HTableInterface mockTable;

    private Result mockResult;

    private ItemSetsDaoImpl itemSetsDao;

    private ArgumentCaptor<Put> putCaptor;
    private ArgumentCaptor<byte[]> rowCaptor;
    private ArgumentCaptor<byte[]> cfCaptor;
    private ArgumentCaptor<byte[]> columnCaptor;
    private ArgumentCaptor<Long> longCaptor;
    private ArgumentCaptor<Object[]> resultsCaptor;
    private ArgumentCaptor<Get> getCaptor;

    @Before
    public void setUp() {
        mockConnection = mock(HConnection.class);
        mockTable = mock(HTableInterface.class);
        mockResult = mock(Result.class);
        itemSetsDao = new ItemSetsDaoImpl(mockConnection);
        putCaptor = ArgumentCaptor.forClass(Put.class);
        rowCaptor = ArgumentCaptor.forClass(byte[].class);
        cfCaptor = ArgumentCaptor.forClass(byte[].class);
        columnCaptor = ArgumentCaptor.forClass(byte[].class);
        longCaptor = ArgumentCaptor.forClass(Long.class);
        resultsCaptor = ArgumentCaptor.forClass(Object[].class);
        getCaptor = ArgumentCaptor.forClass(Get.class);
    }

    @Test
    public void testUpdateCount() throws IOException {
        when(mockConnection.getTable(TABLE_NAME)).thenReturn(mockTable);
        itemSetsDao.updateCount("1-2", 5);
        verify(mockTable).put(putCaptor.capture());

        Put put = putCaptor.getValue();
        assertEquals("Row key", "1-2", Bytes.toString(put.getRow()));
        assertEquals("Count", 5, Bytes.toLong(put.get(COUNT_FAM, COUNT_COL).get(0)
                .getValue()));
    }

    @Test
    public void testIncrementCount() throws IOException {
        when(mockConnection.getTable(TABLE_NAME)).thenReturn(mockTable);
        itemSetsDao.incrementCount("1-2", 5);
        verify(mockTable).incrementColumnValue(rowCaptor.capture(), cfCaptor.capture(), columnCaptor.capture(), longCaptor.capture());

        assertEquals("Row key", "1-2", Bytes.toString(rowCaptor.getValue()));
        assertEquals("CF", Bytes.toString(COUNT_FAM), Bytes.toString(cfCaptor.getValue()));
        assertEquals("Column", Bytes.toString(COUNT_COL), Bytes.toString(columnCaptor.getValue()));
        assertEquals("Count", new Long(5l), longCaptor.getValue());
    }

    @Test
    public void testUpdateAssocCount() throws IOException {
        when(mockConnection.getTable(TABLE_NAME)).thenReturn(mockTable);
        itemSetsDao.updateAssocCount("1-2", "3", 5);
        verify(mockTable).put(putCaptor.capture());

        Put put = putCaptor.getValue();
        assertEquals("Row key", "1-2", Bytes.toString(put.getRow()));
        assertEquals("Count", 5, Bytes.toLong(put.get(ASSOCIATION_FAM, Bytes.toBytes("3")).get(0)
                .getValue()));
    }

    @Test
    public void testIncrementAssocCount() throws IOException {
        when(mockConnection.getTable(TABLE_NAME)).thenReturn(mockTable);
        itemSetsDao.incrementAssocCount("1-2", "3", 5);
        verify(mockTable).incrementColumnValue(rowCaptor.capture(), cfCaptor.capture(), columnCaptor.capture(), longCaptor.capture());

        assertEquals("Row key", "1-2", Bytes.toString(rowCaptor.getValue()));
        assertEquals("CF", Bytes.toString(ASSOCIATION_FAM), Bytes.toString(cfCaptor.getValue()));
        assertEquals("Column", "3", Bytes.toString(columnCaptor.getValue()));
        assertEquals("Count", new Long(5l), longCaptor.getValue());
    }

    @Test
    public void testIncrementItemSetAndAssociations() throws IOException, HBaseException, InterruptedException {
        when(mockConnection.getTable(TABLE_NAME)).thenReturn(mockTable);

        Map<String, Long> assocMap = new HashMap<>();
        assocMap.put("3", 4l);
        itemSetsDao.incrementItemSetAndAssociations("1-2", 5, assocMap);

        ArgumentCaptor<List> listIncrementCaptor = ArgumentCaptor.forClass(List.class);

        verify(mockTable).batch(listIncrementCaptor.capture(), resultsCaptor.capture());

        List<Increment> incrementList = listIncrementCaptor.getValue();

        assertEquals("Row key [0]", "1-2", Bytes.toString(incrementList.get(0).getRow()));
        assertEquals("Row key [1]", "1-2", Bytes.toString(incrementList.get(1).getRow()));
    }

    @Test
    public void testUpdateCounts() throws IOException, HBaseException, InterruptedException {
        when(mockConnection.getTable(TABLE_NAME)).thenReturn(mockTable);

        Map<String, Long> assocMap = new HashMap<>();
        assocMap.put("4", 4l);
        itemSetsDao.updateCounts("1-2", 5, assocMap);

        ArgumentCaptor<List> listPutCaptor = ArgumentCaptor.forClass(List.class);

        verify(mockTable).batch(listPutCaptor.capture(), resultsCaptor.capture());

        List<Put> incrementList = listPutCaptor.getValue();

        assertEquals("Row key [0]", "1-2", Bytes.toString(incrementList.get(0).getRow()));
        assertEquals("Count [0]", 5l, Bytes.toLong(incrementList.get(0).get(COUNT_FAM,
                COUNT_COL).get(0).getValue()));
        assertEquals("Row key [1]", "1-2", Bytes.toString(incrementList.get(1).getRow()));
        assertEquals("Count [1]", 4l, Bytes.toLong(incrementList.get(1).get(ASSOCIATION_FAM,
                Bytes.toBytes("4")).get(0).getValue()));
    }

    @Test
    public void testGetCount() throws IOException {
        when(mockConnection.getTable(TABLE_NAME)).thenReturn(mockTable);
        when(mockTable.get(any(Get.class))).thenReturn(mockResult);

        itemSetsDao.getCount("1-2");

        verify(mockTable).get(getCaptor.capture());

        assertEquals("Row key", "1-2", Bytes.toString(getCaptor.getValue().getRow()));
    }

    @Test
    public void testGetCountAssoc() throws IOException {
        when(mockConnection.getTable(TABLE_NAME)).thenReturn(mockTable);
        when(mockTable.get(any(Get.class))).thenReturn(mockResult);

        itemSetsDao.getCount("1-2", "3");

        verify(mockTable).get(getCaptor.capture());

        assertEquals("Row key", "1-2", Bytes.toString(getCaptor.getValue().getRow()));
    }

    @Test
    public void testGetCountsForAssociations() throws IOException {
        when(mockConnection.getTable(TABLE_NAME)).thenReturn(mockTable);

        List<Recommendation> recommendationList = new ArrayList<>();
        Recommendation recommendation = new Recommendation();
        recommendation.setAssociationId("4");
        recommendationList.add(recommendation);

        recommendation = new Recommendation();
        recommendation.setAssociationId("5");
        recommendationList.add(recommendation);

        when(mockTable.get(any(List.class))).thenReturn(Arrays.asList(mockResult, mockResult).toArray(new Result[2]));

        itemSetsDao.getCountsForAssociations(recommendationList);

        ArgumentCaptor<List> list = ArgumentCaptor.forClass(List.class);

        verify(mockTable).get(list.capture());

        List<Get> getList = list.getValue();
        assertEquals("Size", 2, getList.size());
        assertEquals("Row key [0]", "4", Bytes.toString(getList.get(0).getRow()));
        assertEquals("Row key [1]", "5", Bytes.toString(getList.get(1).getRow()));
    }

    @Test
    public void testGetAssociations() throws IOException {
        when(mockConnection.getTable(TABLE_NAME)).thenReturn(mockTable);

        when(mockTable.get(any(Get.class))).thenReturn(mockResult);
        when(mockResult.getFamilyMap(ASSOCIATION_FAM)).thenReturn(Collections.emptyNavigableMap());

        itemSetsDao.getAssociations("1-2");

        verify(mockTable).get(getCaptor.capture());

        assertEquals("Row key", "1-2", Bytes.toString(getCaptor.getValue().getRow()));
    }
}
