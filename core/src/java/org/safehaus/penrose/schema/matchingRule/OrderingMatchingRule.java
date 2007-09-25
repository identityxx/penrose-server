/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author Endi S. Dewata
 */
public class OrderingMatchingRule {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public final static String CASE_IGNORE        = "caseIgnoreOrderingMatch";
    public final static String CASE_EXACT         = "caseExactOrderingMatch";
    public final static String INTEGER            = "integerOrderingMatch";
    public final static String NUMERIC_STRING     = "numericStringOrderingMatch";
    public final static String OCTET_STRING       = "octetStringOrderingMatch";

    public final static OrderingMatchingRule DEFAULT = new CaseIgnoreOrderingMatchingRule();

    public static Map<String,OrderingMatchingRule> instances = new TreeMap<String,OrderingMatchingRule>();

    static {
        instances.put(CASE_IGNORE,    new CaseIgnoreOrderingMatchingRule());
        instances.put(CASE_EXACT,     new CaseExactOrderingMatchingRule());
        instances.put(INTEGER,        new IntegerOrderingMatchingRule());
        instances.put(NUMERIC_STRING, DEFAULT);
        instances.put(OCTET_STRING,   DEFAULT);
    }

    public static OrderingMatchingRule getInstance(String name) {
        if (name == null) return DEFAULT;

        OrderingMatchingRule orderingMatchingRule = instances.get(name);
        if (orderingMatchingRule == null) return DEFAULT;

        return orderingMatchingRule;
    }

    public int compare(Object object1, Object object2) throws Exception {

        if (debug) {
            log.debug("Comparing:");
            log.debug(" - "+object1+" ("+object1.getClass().getSimpleName()+")");
            log.debug(" - "+object2+" ("+object2.getClass().getSimpleName()+")");
        }

        if (object1 instanceof Integer && object2 instanceof String) {
            Integer i1 = (Integer)object1;
            Integer i2 = Integer.parseInt((String)object2);
            return i1.compareTo(i2);

        } else if (object1 instanceof String && object2 instanceof Integer) {
            Integer i1 = Integer.parseInt((String)object1);
            Integer i2 = (Integer)object2;
            return i1.compareTo(i2);
        }

        Comparable c1 = (Comparable)object1;
        Comparable c2 = (Comparable)object2;
        return c1.compareTo(c2);
    }
}
