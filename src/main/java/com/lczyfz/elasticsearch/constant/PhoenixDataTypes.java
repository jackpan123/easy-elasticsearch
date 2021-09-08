package com.lczyfz.elasticsearch.constant;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * The phoenix data types.
 *
 * @author Jack Pan
 * @version 1.00 2020-09-24
 */
public final class PhoenixDataTypes implements Serializable {

    /**
     * The numeric type set.
     */
    private static final Set<String> NUMERIC_TYPE = new HashSet<>(12);

    /**
     * The date type set.
     */
    private static final Set<String> DATE_TYPE = new HashSet<>(6);

    /**
     * The string type set.
     */
    private static final Set<String> STRING_TYPE = new HashSet<>(2);

    /**
     * Init function.
     */
    static {
        initNumericType();
        initDateType();
        initStringType();
    }

    /**
     * Init numeric type.
     */
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

    /**
     * Init date type.
     */
    private static void initDateType() {
        DATE_TYPE.add("TIME");
        DATE_TYPE.add("DATE");
        DATE_TYPE.add("TIMESTAMP");
        DATE_TYPE.add("UNSIGNED_TIME");
        DATE_TYPE.add("UNSIGNED_DATE");
        DATE_TYPE.add("UNSIGNED_TIMESTAMP");
    }

    /**
     * Init string type.
     */
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
    public static boolean validateNumeric(final String fieldType) {
        return NUMERIC_TYPE.contains(fieldType);
    }

    /**
     * Validate date.
     *
     * @param fieldType The field type.
     * @return Boolean result.
     */
    public static boolean validateDate(final String fieldType) {
        return DATE_TYPE.contains(fieldType);
    }

    /**
     * Validate string.
     *
     * @param fieldType The field type.
     * @return Boolean result.
     */
    public static boolean validateString(final String fieldType) {
        return STRING_TYPE.contains(fieldType);
    }
}
