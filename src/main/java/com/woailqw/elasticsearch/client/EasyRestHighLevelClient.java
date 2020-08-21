package com.woailqw.elasticsearch.client;

import com.alibaba.fastjson.JSONObject;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Easy rest high level client.
 *
 * @author Jack Pan
 * @version 1.00 2020-08-17
 */
public final class EasyRestHighLevelClient implements Closeable {

    /**
     * Internal client.
     */
    private RestHighLevelClient client;

    /**
     * Default type.
     */
    private static final String DEFAULT_TYPE = "_doc";

    /**
     * Properties.
     */
    private static final String PROPERTIES = "properties";

    /**
     * Type.
     */
    private static final String TYPE = "type";

    /**
     * Default timeout.
     */
    private static final String DEFAULT_TIMEOUT = "1m";

    /**
     * Query string search format.
     */
    private static final String QUERY_STRING_FORMAT = "*%s*";

    /**
     * Total number field.
     */
    private static final String TOTAL_HITS = "totalHits";

    /**
     * Data list field.
     */
    private static final String DATA_LIST = "dataList";

    /**
     * Traditional database data type mapping elasticsearch data type.
     */
    private static final Map<String, Map<String, String>> DATA_TYPE_MAPPING =
        new ConcurrentHashMap<>(7);

    /**
     * Data type initialization.
     */
    static {
        DATA_TYPE_MAPPING.put(
            "VARCHAR", elasticsearchDataTypeFormatter("text")
        );
        DATA_TYPE_MAPPING.put(
            "CHAR", elasticsearchDataTypeFormatter("keyword")
        );
        DATA_TYPE_MAPPING.put(
            "DATE", elasticsearchDataTypeFormatter("date")
        );
        DATA_TYPE_MAPPING.put(
            "INTEGER", elasticsearchDataTypeFormatter("long")
        );
        DATA_TYPE_MAPPING.put(
            "BIGINT", elasticsearchDataTypeFormatter("long")
        );
        DATA_TYPE_MAPPING.put(
            "BOOLEAN", elasticsearchDataTypeFormatter("boolean")
        );
        DATA_TYPE_MAPPING.put(
            "DOUBLE", elasticsearchDataTypeFormatter("double")
        );
    }

    /**
     * Traditional database data type convert to elasticsearch
     * data type.
     *
     * @param elasticsearchDataType Elasticsearch data type.
     * @return Elasticsearch data type map.
     */
    private static Map<String, String> elasticsearchDataTypeFormatter(
        final String elasticsearchDataType) {

        Map<String, String> field = new HashMap<>(1);
        field.put(TYPE, elasticsearchDataType);
        return field;
    }

    /**
     * Easy rest client constructor.
     *
     * @param httpHosts Http host configuration.
     */
    public EasyRestHighLevelClient(final HttpHost... httpHosts) {
        this.client = new RestHighLevelClient(RestClient.builder(httpHosts));
    }

    /**
     * Get internal client.
     * @return RestHighLevelClient
     */
    public RestHighLevelClient getInternalClient() {
        return this.client;
    }

    /**
     * Traditional database mapping elasticsearch index.
     *
     * @param database Traditional database name.
     * @param table Table name.
     * @param fieldMapping Field mapping.
     * @return Index name.
     * @throws IOException If something goes wrong.
     */
    public String indexMapping(final String database, final String table,
        final Map<String, String> fieldMapping) throws IOException {

        String indexName = this.uniqueIndex(
            database.toLowerCase(), table.toLowerCase()
        );
        CreateIndexRequest request = new CreateIndexRequest(indexName);

        request.settings(Settings.builder()
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 2)
        );

        Map<String, Object> jsonMap = new HashMap<>(1);
        jsonMap.put(DEFAULT_TYPE, this.incrementProperties(fieldMapping));
        request.mapping(DEFAULT_TYPE, jsonMap);
        request.timeout(DEFAULT_TIMEOUT);

        AcknowledgedResponse response =
            this.client.indices().create(request, RequestOptions.DEFAULT);

        return indexName;
    }

    /**
     * Dump data to elasticsearch.
     *
     * @param indexName The index name of elasticsearch.
     * @param dataList Data list.
     * @return Bulk Response.
     * @throws IOException If something goes wrong.
     */
    public BulkResponse dump(final String indexName,
        final List<Map<String, String>> dataList) throws IOException {

        BulkRequest request = new BulkRequest();
        request.timeout(DEFAULT_TIMEOUT);
        dataList.forEach(
            data -> request.add(
                    new IndexRequest(indexName, DEFAULT_TYPE).source(data)
                )
        );
        return this.client.bulk(request, RequestOptions.DEFAULT);
    }

    /**
     * Create index with a certain name.
     *
     * @param indexName The index name.
     * @return Create response.
     * @throws IOException If something goes wrong.
     */
    public CreateIndexResponse createIndex(final String indexName)
        throws IOException {

        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 2)
        );
        request.timeout(DEFAULT_TIMEOUT);
        return this.client.indices().create(request, RequestOptions.DEFAULT);

    }

    /**
     * Delete index with a certain name.
     *
     * @param indexName The index name.
     * @return Delete response.
     * @throws IOException If something goes wrong.
     */
    public AcknowledgedResponse deleteIndex(final String indexName)
        throws IOException {

        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        request.timeout(DEFAULT_TIMEOUT);
        return this.client.indices().delete(request, RequestOptions.DEFAULT);

    }

    /**
     * The comprehensive search for index list.
     *
     * @param keyword Keyword.
     * @param indexList The index list.
     * @return Json format data.
     * @throws IOException If something goes wrong.
     */
    public JSONObject comprehensiveSearch(final String keyword,
        final List<String> indexList) throws IOException {

        JSONObject result = new JSONObject(new HashMap<>(indexList.size()));
        for (final String indexName : indexList) {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders
                .queryStringQuery(String
                    .format(QUERY_STRING_FORMAT, keyword)));

            searchRequest.source(builder);
            SearchResponse searchResponse =
                this.client.search(searchRequest, RequestOptions.DEFAULT);
            // Deal with response data.
            SearchHits hits = searchResponse.getHits();
            Map<String, Object> data = new HashMap<>(2);
            List<Map<String, Object>> dataList = new LinkedList<>();
            data.put(TOTAL_HITS, hits.getTotalHits());
            data.put(DATA_LIST, dataList);
            for (final SearchHit searchHit : hits.getHits()) {
                dataList.add(searchHit.getSourceAsMap());
            }

            result.put(indexName, data);
        }

        return result;
    }

    /**
     * Create elasticsearch properties.
     *
     * @param fieldMapping Traditional database data type.
     * @return Properties map.
     */
    public Map<String, Object> incrementProperties(
        final Map<String, String> fieldMapping) {

        Map<String, Map<String, String>> properties =
            new HashMap<>(fieldMapping.size());

        fieldMapping.forEach(
            (fieldName, dataType)
                -> properties.put(fieldName, DATA_TYPE_MAPPING.get(dataType))
        );
        Map<String, Object> jsonMap = new HashMap<>(1);
        jsonMap.put(PROPERTIES, properties);

        return jsonMap;
    }

    /**
     * Create unique index.
     *
     * @param database Traditional database name.
     * @param table Table name.
     * @return Index name.
     */
    private String uniqueIndex(final String database, final String table) {
        return database.concat("_")
            .concat(table).concat("_")
            .concat(UUID.randomUUID().toString().replaceAll("-", ""));
    }

    /**
     * Closes this stream and releases any system resources associated with it.
     * If the stream is already closed then invoking this method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention.
     * It is strongly advised to relinquish the underlying
     * resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        this.client.close();
    }
}
