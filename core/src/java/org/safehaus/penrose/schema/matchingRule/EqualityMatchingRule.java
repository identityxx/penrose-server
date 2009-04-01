/**
 * Copyright 2009 Red Hat, Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.schema.matchingRule;

import org.safehaus.penrose.util.BinaryUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Map;
import java.util.TreeMap;
import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class EqualityMatchingRule {

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static String BOOLEAN            = "booleanMatch";
    public final static String CASE_IGNORE        = "caseIgnoreMatch";
    public final static String CASE_EXACT         = "caseExactMatch";
    public final static String DISTINGUISHED_NAME = "distinguishedNameMatch";
    public final static String INTEGER            = "integerMatch";
    public final static String NUMERIC_STRING     = "numericStringMatch";
    public final static String OCTET_STRING       = "octetStringMatch";
    public final static String OBJECT_IDENTIFIER  = "objectIdentiferMatch";

    public final static EqualityMatchingRule DEFAULT = new CaseIgnoreEqualityMatchingRule();

    public static Map<String,EqualityMatchingRule> instances = new TreeMap<String,EqualityMatchingRule>();

    static {
        instances.put(BOOLEAN,            new BooleanEqualityMatchingRule());
        instances.put(CASE_IGNORE,        new CaseIgnoreEqualityMatchingRule());
        instances.put(CASE_EXACT,         new CaseExactEqualityMatchingRule());
        instances.put(DISTINGUISHED_NAME, new DistinguishedNameEqualityMatchingRule());
        instances.put(INTEGER,            new IntegerEqualityMatchingRule());
        instances.put(NUMERIC_STRING,     DEFAULT);
        instances.put(OCTET_STRING,       DEFAULT);
        instances.put(OBJECT_IDENTIFIER,  DEFAULT);
    }

    public static EqualityMatchingRule getInstance(String name) {
        if (name == null) return DEFAULT;

        EqualityMatchingRule equalityMatchingRule = instances.get(name);
        if (equalityMatchingRule == null) return DEFAULT;

        return equalityMatchingRule;
    }

    public boolean compare(Object object1, Object object2) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (object1 == null && object2 == null) return true;
        if (object1 == null || object2 == null) return false;

        if (object1 instanceof Integer && object2 instanceof String) {
            object2 = Integer.parseInt((String)object2);
            
            if (debug) {
                log.debug("Comparing:");
                log.debug(" - "+object1);
                log.debug(" - "+object2);
            }

            return object1.equals(object2);
        }

        if (object1 instanceof String && object2 instanceof Integer) {
            object1 = Integer.parseInt((String)object1);

            if (debug) {
                log.debug("Comparing:");
                log.debug(" - "+object1);
                log.debug(" - "+object2);
            }

            return object1.equals(object2);
        }

        if (object1 instanceof byte[] && object2 instanceof byte[]) {
            byte[] bytes1 = (byte[])object1;
            byte[] bytes2 = (byte[])object2;

            if (debug) {
                log.debug("Comparing:");
                log.debug(" - "+BinaryUtil.encode(BinaryUtil.BIG_INTEGER, bytes1));
                log.debug(" - "+BinaryUtil.encode(BinaryUtil.BIG_INTEGER, bytes2));
            }

            return Arrays.equals(bytes1, bytes2);
        }

        if (object1 instanceof byte[] && object2 instanceof String) {
            byte[] bytes1 = (byte[])object1;
            byte[] bytes2 = ((String)object2).getBytes();

            if (debug) {
                log.debug("Comparing:");
                log.debug(" - "+BinaryUtil.encode(BinaryUtil.BIG_INTEGER, bytes1));
                log.debug(" - "+BinaryUtil.encode(BinaryUtil.BIG_INTEGER, bytes2));
            }

            return Arrays.equals(bytes1, bytes2);
        }

        if (object1 instanceof String && object2 instanceof byte[]) {
            byte[] bytes1 = ((String)object1).getBytes();
            byte[] bytes2 = (byte[])object2;

            if (debug) {
                log.debug("Comparing:");
                log.debug(" - "+BinaryUtil.encode(BinaryUtil.BIG_INTEGER, bytes1));
                log.debug(" - "+BinaryUtil.encode(BinaryUtil.BIG_INTEGER, bytes2));
            }

            return Arrays.equals(bytes1, bytes2);
        }

        if (debug) {
            log.debug("Comparing:");
            log.debug(" - "+object1);
            log.debug(" - "+object2);
        }

        return object1.equals(object2);
    }
}
