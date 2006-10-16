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

    Logger log = LoggerFactory.getLogger(getClass());

    public final static String CASE_IGNORE        = "caseIgnoreOrderingMatch";
    public final static String CASE_EXACT         = "caseExactOrderingMatch";
    public final static String INTEGER            = "integerOrderingMatch";
    public final static String NUMERIC_STRING     = "numericStringOrderingMatch";
    public final static String OCTET_STRING       = "octetStringOrderingMatch";

    public final static OrderingMatchingRule DEFAULT = new CaseIgnoreOrderingMatchingRule();

    public static Map instances = new TreeMap();

    static {
        instances.put(CASE_IGNORE,    new CaseIgnoreOrderingMatchingRule());
        instances.put(CASE_EXACT,     new CaseExactOrderingMatchingRule());
        instances.put(INTEGER,        new IntegerOrderingMatchingRule());
        instances.put(NUMERIC_STRING, DEFAULT);
        instances.put(OCTET_STRING,   DEFAULT);
    }

    public static OrderingMatchingRule getInstance(String name) {
        if (name == null) return DEFAULT;

        OrderingMatchingRule orderingMatchingRule = (OrderingMatchingRule)instances.get(name);
        if (orderingMatchingRule == null) return DEFAULT;

        return orderingMatchingRule;
    }

    public int compare(Object object1, Object object2) throws Exception {
        Comparable c1 = (Comparable)object1;
        Comparable c2 = (Comparable)object2;
        return c1.compareTo(c2);
    }
}
