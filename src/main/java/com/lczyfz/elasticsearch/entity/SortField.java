package com.lczyfz.elasticsearch.entity;

/**
 * @author jackpan
 * @version v1.0 2021/8/23 11:38
 */
public class SortField {

    /**
     * The field name.
     */
    private String fieldName;

    /**
     * sort type asc， desc
     */
    private String sortType;

    /**
     * Gets fieldName.
     *
     * @return Value of fieldName.
     */
    public String getFieldName() {
        return this.fieldName;
    }

    /**
     * Sets fieldName.
     *
     * @param fieldName Simple param.
     */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Gets sortType.
     *
     * @return Value of sortType.
     */
    public String getSortType() {
        return this.sortType;
    }

    /**
     * Sets sortType.
     *
     * @param sortType Simple param.
     */
    public void setSortType(String sortType) {
        this.sortType = sortType;
    }
}
