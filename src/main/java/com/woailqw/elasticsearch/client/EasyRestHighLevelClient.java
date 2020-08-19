package com.woailqw.elasticsearch.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;

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
