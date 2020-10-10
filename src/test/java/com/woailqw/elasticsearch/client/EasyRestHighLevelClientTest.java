package com.woailqw.elasticsearch.client;

import com.alibaba.fastjson.JSONObject;
import com.woailqw.elasticsearch.entity.AdvancedSearchCondition;
import com.woailqw.elasticsearch.entity.SearchField;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Easy rest high level client test.
 *
 * @author Jack Pan
 * @version 1.00 2020-08-17
 */
public final class EasyRestHighLevelClientTest {

    /**
     * Test elasticsearch connection.
     */
    @Test
    public void restHighClientConnectionTest() {
        EasyRestHighLevelClient client = new EasyRestHighLevelClient(
            new HttpHost("192.168.101.17", 9200, "http")
        );

        Assert.assertNotNull(client.getInternalClient());
    }

    /**
     * Test create index method and delete index method.
     * @throws IOException If something goes wrong.
     */
    @Test
    public void createAndDeleteIndexTest() throws IOException {
        EasyRestHighLevelClient client = new EasyRestHighLevelClient(
            new HttpHost("192.168.101.17", 9200, "http")
        );
        CreateIndexResponse response = client.createIndex("jack_create");
        Assert.assertTrue(response.isAcknowledged());
        AcknowledgedResponse deleteResponse =
            client.deleteIndex("jack_create");
        Assert.assertTrue(deleteResponse.isAcknowledged());
    }

    /**
     * Test dump.
     *
     * @throws IOException If something goes wrong.
     */
    @Test
    public void dumpTest() throws IOException {
        EasyRestHighLevelClient client = new EasyRestHighLevelClient(
            new HttpHost("192.168.101.17", 9200, "http")
        );
        List<Map<String, String>> list = new ArrayList<>(1);
        Map<String, String> dataOne = new HashMap<>(1);
        dataOne.put("name", "jackPan");
        list.add(dataOne);
        BulkResponse response = client.dump("jack_pan_test", list);

        Assert.assertFalse(response.hasFailures());
    }

    /**
     * Comprehensive test.
     *
     * @throws IOException If something goes wrong.
     */
    @Test
    public void comprehensiveTest() throws IOException {
        EasyRestHighLevelClient client = new EasyRestHighLevelClient(
            new HttpHost("192.168.101.17", 9200, "http")
        );

        List<String> indexList = new ArrayList<>(1);
        indexList.add("jack_pan_test");
        JSONObject result = client.comprehensiveSearch("jack", indexList);
        Assert.assertNotNull(result);
        Map<String, Object> indexData =
            (Map<String, Object>)result.get("jack_pan_test");
        Assert.assertEquals(2, indexData.size());
        List<Map<String, Object>> dataList =
            (List<Map<String, Object>>)indexData.get("singleIndexData");
        Assert.assertTrue(dataList.size() > 0);
    }

    /**
     * Comprehensive page search test.
     *
     * @throws IOException If something goes wrong.
     */
    @Test
    public void comprehensivePageSearchTest() throws IOException {
        EasyRestHighLevelClient client = new EasyRestHighLevelClient(
            new HttpHost("192.168.101.17", 9200, "http")
        );

        JSONObject result = client
            .comprehensiveSearch("jack", "jack_pan_test", 2, 10);
        Assert.assertNotNull(result);
        List<Map<String, Object>> dataList =
            (List<Map<String, Object>>)result.get("singleIndexData");
        Assert.assertTrue(dataList.size() > 0);
    }

    /**
     * Get document test.
     *
     * @throws IOException If something goes wrong.
     */
    @Test
    public void getDocumentTest() throws IOException {
        EasyRestHighLevelClient client = new EasyRestHighLevelClient(
            new HttpHost("192.168.101.17", 9200, "http")
        );

        GetResponse response = client.getDocument("jack_pan_test", "1");
        Map<String, Object> sourceAsMap = response.getSourceAsMap();
        Assert.assertNotNull(sourceAsMap);
        Assert.assertTrue("jackPan".equals(sourceAsMap.get("full_name")));
    }

    /**
     * Advanced search test.
     *
     * @throws IOException If something goes wrong.
     * @throws ParseException If something goes wrong.
     */
    @Test
    public void advancedSearchTest() throws IOException, ParseException {
        AdvancedSearchCondition condition = new AdvancedSearchCondition();
        condition.setIndexName("hfkajshdgkjhasdghoaidshgoudsajjaogiodshgoa_728"
            + "732_aa_2d1f115fc51741fb8c6afc77b86ccd3d");

        List<SearchField> fieldList = new ArrayList<>();
        SearchField dateField = new SearchField();
        dateField.setFieldName("AA_PK");
        dateField.setMethod(">");
        dateField.setTypeName("BIGINT");
        dateField.setValue("3");

        fieldList.add(dateField);

//        SearchField numField = new SearchField();
//        numField.setFieldName("LDH_WORKFLOWID");
//        numField.setMethod("=");
//        numField.setTypeName("INTEGER");
//        numField.setValue("213309698374365184");
//
//        fieldList.add(numField);

        condition.setSearchMethod(fieldList);
        EasyRestHighLevelClient client = new EasyRestHighLevelClient(
            new HttpHost("192.168.101.17", 9200, "http")
        );

        JSONObject search = client.advancedSearch(condition, 1, 10);
        Assert.assertNotNull(search);

    }
}
