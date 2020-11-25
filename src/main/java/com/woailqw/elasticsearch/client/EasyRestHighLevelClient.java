package com.woailqw.elasticsearch.client;

import static com.woailqw.elasticsearch.constant.MatchMethod.*;

import com.alibaba.fastjson.JSONObject;
import com.woailqw.elasticsearch.constant.PhoenixDataTypes;
import com.woailqw.elasticsearch.entity.AdvancedSearchCondition;
import com.woailqw.elasticsearch.entity.SearchField;
import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.Scroll;
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
    public static final String SINGLE_INDEX_TOTAL = "singleIndexTotal";

    /**
     * Data list field.
     */
    public static final String SINGLE_INDEX_DATA = "singleIndexData";

    /**
     * Traditional database data type mapping elasticsearch data type.
     */
    private static final Map<String, Map<String, String>> DATA_TYPE_MAPPING =
        new ConcurrentHashMap<>(7);

    /**
     * Default scroll setting.
     */
    private static final Scroll DEFAULT_SCROLL =
        new Scroll(TimeValue.timeValueMinutes(1L));

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
     * Date formatter.
     */
    private static final SimpleDateFormat DATE_FORMAT =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
            builder.query(this.crateQuery(keyword));

            searchRequest.source(builder);
            SearchResponse searchResponse =
                this.client.search(searchRequest, RequestOptions.DEFAULT);
            // Deal with response data.
            result.put(indexName,
                this.extraSearchHits(searchResponse.getHits()));
        }

        return result;
    }

    /**
     * The comprehensive page search for single index.
     *
     * @param keyword The keyword.
     * @param indexName The index name.
     * @param pageNo The page number.
     * @param pageSize The page size.
     * @return Single index page search result.
     * @throws IOException If something goes wrong.
     */
    public JSONObject comprehensiveSearch(final String keyword,
        final String indexName, final Integer pageNo, final Integer pageSize)
        throws IOException {

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        searchRequest.scroll(DEFAULT_SCROLL);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(this.crateQuery(keyword));
        searchSourceBuilder.size(pageSize);
        searchRequest.source(searchSourceBuilder);

        SearchHits searchHits = this.scrollSearch(searchRequest, pageNo);
        return this.extraSearchHits(searchHits);
    }

    /**
     * Advanced search.
     *
     * @param condition The user selected condition.
     * @param pageNo The page number.
     * @param pageSize The page size.
     * @return Result or return null if condition doesn't meet specifications.
     * @throws IOException If something goes wrong.
     * @throws ParseException If something goes wrong.
     */
    public JSONObject advancedSearch(final AdvancedSearchCondition condition,
        final Integer pageNo, final Integer pageSize)
        throws IOException, ParseException {

        // Create bool query.
        BoolQueryBuilder advantageQuery = QueryBuilders.boolQuery();

        // Extra condition.
        for (final SearchField field : condition.getSearchMethod()) {
            QueryBuilder singleQuery = this.crateQuery(field);

            if (singleQuery != null) {
                if (NOT_EQUALS.equals(field.getMethod())) {
                    advantageQuery.mustNot().add(singleQuery);
                } else {
                    advantageQuery.must().add(singleQuery);
                }
            }

        }

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(condition.getIndexName());
        searchRequest.scroll(DEFAULT_SCROLL);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(advantageQuery);
        searchSourceBuilder.size(pageSize);
        searchRequest.source(searchSourceBuilder);

        // Execute query.
        SearchHits searchHits = this.scrollSearch(searchRequest, pageNo);
        return this.extraSearchHits(searchHits);
    }

    /**
     * Create date query.
     *
     * @param field Search field.
     * @return Query.
     * @throws ParseException If something goes wrong.
     */
    private QueryBuilder crateQuery(final SearchField field)
        throws ParseException {
        QueryBuilder singleQuery = null;
        String typeName = field.getTypeName().toUpperCase().trim();
        if (PhoenixDataTypes.validateDate(typeName)) {
            // Create date query.
            singleQuery = this.createDateQuery(field);
        } else if (PhoenixDataTypes.validateNumeric(typeName)) {
            // Create numeric query.
            singleQuery = this.createNumericQuery(field);
        } else if (PhoenixDataTypes.validateString(typeName)) {
            // Create string query.
            singleQuery = this.createStringQuery(field);
        }

        return singleQuery;
    }

    /**
     * Create date query.
     *
     * @param field Search field.
     * @return Date query.
     * @throws ParseException If somethings goes wrong.
     */
    private QueryBuilder createDateQuery(final SearchField field)
        throws ParseException {

        QueryBuilder simpleQuery = null;
        RangeQueryBuilder dateBuilder =
            QueryBuilders.rangeQuery(field.getFieldName());

        long beginTime = DATE_FORMAT.parse(field.getBeginTime()).getTime();
        long endTime = 0;
        if (field.getEndTime() != null) {
            endTime = DATE_FORMAT.parse(field.getEndTime()).getTime();
        }
        String method = field.getMethod();

        // Build query.
        if (EQUALS.equals(method) || NOT_EQUALS.equals(method)) {
            simpleQuery = dateBuilder.gte(beginTime).lte(beginTime);
        } else if (GT.equals(method)) {
            simpleQuery = dateBuilder.gt(beginTime);
        } else if (LT.equals(method)) {
            simpleQuery = dateBuilder.lt(beginTime);
        } else if (GTE.equals(method)) {
            simpleQuery = dateBuilder.gte(beginTime);
        } else if (LTE.equals(method)) {
            simpleQuery = dateBuilder.lte(beginTime);
        } else if (RANGE.equals(method)) {
            simpleQuery = dateBuilder.gte(beginTime).lte(endTime);
        }

        return simpleQuery;
    }


    /**
     * Create numeric query.
     *
     * @param field Search field.
     * @return Search query.
     */
    private QueryBuilder createNumericQuery(final SearchField field) {

        QueryBuilder simpleQuery = null;
        String method = field.getMethod();
        String fieldName = field.getFieldName();
        String value = field.getValue();

        // Build query.
        if (EQUALS.equals(method) || NOT_EQUALS.equals(method)) {
            simpleQuery = QueryBuilders.termQuery(fieldName, value);
        } else if (GT.equals(method)) {
            simpleQuery = QueryBuilders.rangeQuery(fieldName).gt(value);
        } else if (LT.equals(method)) {
            simpleQuery = QueryBuilders.rangeQuery(fieldName).lt(value);
        } else if (GTE.equals(method)) {
            simpleQuery = QueryBuilders.rangeQuery(fieldName).gte(value);
        } else if (LTE.equals(method)) {
            simpleQuery = QueryBuilders.rangeQuery(fieldName).lte(value);
        }

        return simpleQuery;

    }

    /**
     * Create string query.
     *
     * @param field Search field.
     * @return Query.
     */
    private QueryBuilder createStringQuery(final SearchField field) {

        QueryBuilder simpleQuery = null;
        String method = field.getMethod();
        String fieldName = field.getFieldName();
        String value = field.getValue();

        // Build query.
        if (EQUALS.equals(method) || NOT_EQUALS.equals(method)) {
            simpleQuery = QueryBuilders.termQuery(fieldName, value);
        } else if (CONTAINS.equals(method)) {
            simpleQuery = QueryBuilders.matchPhraseQuery(fieldName, value);
        } else if (LIKE.equals(method)) {
            simpleQuery = QueryBuilders.wildcardQuery(fieldName, "*" + value + "*");
        }

        return simpleQuery;
    }


    /**
     * Generate query statements based on keywords.
     *
     * @param keyword The keyword.
     * @return QueryBuilder.
     */
    private QueryBuilder crateQuery(final String keyword) {
        QueryBuilder query = null;
        if (keyword == null || "".equals(keyword)) {
            query = QueryBuilders.matchAllQuery();
        } else {
            query = QueryBuilders
                .queryStringQuery(String
                    .format(QUERY_STRING_FORMAT, keyword));
        }

        return query;
    }

    /**
     * Get document detail by id.
     *
     * @param indexName The index name.
     * @param docId Document id
     * @throws IOException If something goes wrong.
     * @return Get response.
     */
    public GetResponse getDocument(final String indexName, final String docId)
        throws IOException {

        GetRequest request = new GetRequest(indexName, DEFAULT_TYPE, docId);
        GetResponse getResponse = this.client
            .get(request, RequestOptions.DEFAULT);
        return getResponse;
    }

    /**
     * Extra search hits data.
     *
     * @param searchHits Search result.
     * @return Extra result.
     */
    private JSONObject extraSearchHits(final SearchHits searchHits) {
        JSONObject result = new JSONObject(new HashMap<>(2));
        List<Map<String, Object>> dataList = new LinkedList<>();
        result.put(SINGLE_INDEX_TOTAL, searchHits.getTotalHits());
        result.put(SINGLE_INDEX_DATA, dataList);
        for (final SearchHit searchHit : searchHits.getHits()) {
            dataList.add(searchHit.getSourceAsMap());
        }

        return result;
    }

    /**
     * Use scroll search.
     *
     * @param request The search request.
     * @param pageNo The page number.
     * @return The search result.
     * @throws IOException If something goes wrong.
     */
    public SearchHits scrollSearch(final SearchRequest request,
        final Integer pageNo) throws IOException {
        // execute search
        SearchResponse searchResponse =
            this.client.search(request, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHits searchHits = searchResponse.getHits();

        // scroll search
        if (searchHits.getHits() != null && searchHits.getHits().length > 0) {
            int i= 1;
            while (i < pageNo) {
                SearchScrollRequest scrollRequest =
                    new SearchScrollRequest(scrollId);
                scrollRequest.scroll(DEFAULT_SCROLL);
                searchResponse =
                    this.client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits();
                i++;
            }
        }

        // clear scroll session
        this.clearScrollSession(scrollId);

        return searchHits;
    }
    /**
     * Clear scroll id.
     *
     * @param scrollId The scroll search ID.
     * @throws IOException If something goes wrong.
     */
    private void clearScrollSession(final String scrollId) throws IOException {
        // clear scroll session
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        this.client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
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
