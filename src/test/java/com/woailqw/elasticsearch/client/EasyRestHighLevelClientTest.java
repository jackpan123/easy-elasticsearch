package com.woailqw.elasticsearch.client;

import org.apache.http.HttpHost;
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
    //@Test
    public void restHighClientConnectionTest() {
        EasyRestHighLevelClient client = new EasyRestHighLevelClient(
            new HttpHost("Elasticsearch IP", 9200, "http")
        );

        Assert.assertNotNull(client.getInternalClient());
    }
}
