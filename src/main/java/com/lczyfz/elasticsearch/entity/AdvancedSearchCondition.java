package com.lczyfz.elasticsearch.entity;

import java.io.Serializable;
import java.util.List;

/**
 * Advantage search condition.
 *
 * @author Jack Pan
 * @version 1.00
 */
public final class AdvancedSearchCondition implements Serializable {

    /**
     * The index name.
     */
    private String[] indexName;

    /**
     * The search method.
     */
    private List<SearchField> searchMethod;

    /**
     * sort type.
     */
    private SortField sortField;

    /**
     * Gets indexName.
     *
     * @return Value of indexName.
     */
    public String[] getIndexName() {
        return this.indexName;
    }

    /**
     * Sets indexName.
     *
     * @param indexName Simple param.
     */
    public void setIndexName(final String... indexName) {
        this.indexName = indexName;
    }

    /**
     * Gets searchMethod.
     *
     * @return Value of searchMethod.
     */
    public List<SearchField> getSearchMethod() {
        return this.searchMethod;
    }

    /**
     * Sets searchMethod.
     *
     * @param searchMethod Simple param.
     */
    public void setSearchMethod(final List<SearchField> searchMethod) {
        this.searchMethod = searchMethod;
    }

    /**
     * Gets sortField.
     *
     * @return Value of sortField.
     */
    public SortField getSortField() {
        return this.sortField;
    }

    /**
     * Sets sortField.
     *
     * @param sortField Simple param.
     */
    public void setSortField(SortField sortField) {
        this.sortField = sortField;
    }
}
