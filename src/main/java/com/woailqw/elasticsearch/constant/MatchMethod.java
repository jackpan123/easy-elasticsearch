package com.woailqw.elasticsearch.constant;

/**
 * The match method class.
 *
 * @author Jack Pan
 * @version 1.0 2020-09-27
 */
public final class MatchMethod {

    /**
     * Hide constructor.
     */
    private MatchMethod() {
    }

    /**
     * Equals.
     */
    public static final String EQUALS = "=";

    /**
     * Not equals.
     */
    public static final String NOT_EQUALS = "!=";

    /**
     * Greater than.
     */
    public static final String GT = ">";

    /**
     * Less than.
     */
    public static final String LT = "<";

    /**
     * Greater than equals.
     */
    public static final String GTE = ">=";

    /**
     * Less than equals.
     */
    public static final String LTE = "<=";

    /**
     * Range.
     */
    public static final String RANGE = "range";

    /**
     * Contains.
     */
    public static final String CONTAINS = "contains";

    /**
     * like.
     */
    public static final String LIKE = "like";
}
