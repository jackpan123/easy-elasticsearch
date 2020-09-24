package com.woailqw.elasticsearch.entity;

/**
 * The search field for advantage.
 *
 * @author Jack Pan
 * @version 2020-09
 */
public final class SearchField {

    /**
     * The field name.
     */
    private String fieldName;

    /**
     * The type name.
     */
    private String typeName;

    /**
     * The method.
     */
    private String method;

    /**
     * The value.
     */
    private String value;

    /**
     * The begin time.
     */
    private String beginTime;

    /**
     * The end time.
     */
    private String endTime;

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
    public void setFieldName(final String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Gets typeName.
     *
     * @return Value of typeName.
     */
    public String getTypeName() {
        return this.typeName;
    }

    /**
     * Sets typeName.
     *
     * @param typeName Simple param.
     */
    public void setTypeName(final String typeName) {
        this.typeName = typeName;
    }

    /**
     * Gets method.
     *
     * @return Value of method.
     */
    public String getMethod() {
        return this.method;
    }

    /**
     * Sets method.
     *
     * @param method Simple param.
     */
    public void setMethod(final String method) {
        this.method = method;
    }

    /**
     * Gets value.
     *
     * @return Value of value.
     */
    public String getValue() {
        return this.value;
    }

    /**
     * Sets value.
     *
     * @param value Simple param.
     */
    public void setValue(final String value) {
        this.value = value;
    }

    /**
     * Gets beginTime.
     *
     * @return Value of beginTime.
     */
    public String getBeginTime() {
        return this.beginTime;
    }

    /**
     * Sets beginTime.
     *
     * @param beginTime Simple param.
     */
    public void setBeginTime(final String beginTime) {
        this.beginTime = beginTime;
    }

    /**
     * Gets endTime.
     *
     * @return Value of endTime.
     */
    public String getEndTime() {
        return this.endTime;
    }

    /**
     * Sets endTime.
     *
     * @param endTime Simple param.
     */
    public void setEndTime(final String endTime) {
        this.endTime = endTime;
    }
}
