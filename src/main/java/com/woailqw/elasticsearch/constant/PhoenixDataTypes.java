package com.woailqw.elasticsearch.constant;

import java.util.HashSet;
import java.util.Set;

/**
 * The phoenix data types.
 *
 * @author Jack Pan
 * @version 1.00 2020-09-24
 */
public final class PhoenixDataTypes {

    /**
     * The numeric type set.
     */
    private final static Set<String> NUMERIC_TYPE = new HashSet<>(12);

    /**
     * The date type set.
     */
    private final static Set<String> DATE_TYPE = new HashSet<>(6);

    /**
     * The string type set.
     */
    private final static Set<String> STRING_TYPE = new HashSet<>(2);

    static {
        initNumericType();
        initDateType();
        initStringType();
    }

    private static void initNumericType() {
        NUMERIC_TYPE.add("INTEGER");
        NUMERIC_TYPE.add("UNSIGNED_INT");
        NUMERIC_TYPE.add("BIGINT");
        NUMERIC_TYPE.add("UNSIGNED_LONG");
        NUMERIC_TYPE.add("UNSIGNED_TINYINT");
        NUMERIC_TYPE.add("SMALLINT");
        NUMERIC_TYPE.add("UNSIGNED_SMALLINT");
        NUMERIC_TYPE.add("FLOAT");
        NUMERIC_TYPE.add("UNSIGNED_FLOAT");
        NUMERIC_TYPE.add("DOUBLE");
        NUMERIC_TYPE.add("UNSIGNED_DOUBLE");
        NUMERIC_TYPE.add("DECIMAL");
    }

    private static void initDateType() {
        DATE_TYPE.add("TIME");
        DATE_TYPE.add("DATE");
        DATE_TYPE.add("TIMESTAMP");
        DATE_TYPE.add("UNSIGNED_TIME");
        DATE_TYPE.add("UNSIGNED_DATE");
        DATE_TYPE.add("UNSIGNED_TIMESTAMP");
    }

    private static void initStringType() {
        STRING_TYPE.add("VARCHAR");
        STRING_TYPE.add("CHAR");
    }

    /**
     * Hide constructor.
     */
    private PhoenixDataTypes() {}

    /**
     * Validate numeric.
     *
     * @param fieldType The field type.
     * @return Boolean result.
     */
    public static boolean validateNumeric(String fieldType) {
        return NUMERIC_TYPE.contains(fieldType);
    }

    /**
     * Validate date.
     *
     * @param fieldType The field type.
     * @return Boolean result.
     */
    public static boolean validateDate(String fieldType) {
        return DATE_TYPE.contains(fieldType);
    }

    /**
     * Validate string.
     *
     * @param fieldType The field type.
     * @return Boolean result.
     */
    public static boolean validateString(String fieldType) {
        return STRING_TYPE.contains(fieldType);
    }
}
