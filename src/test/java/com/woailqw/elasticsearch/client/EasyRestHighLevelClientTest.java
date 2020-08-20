package com.woailqw.elasticsearch.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkResponse;
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
}
