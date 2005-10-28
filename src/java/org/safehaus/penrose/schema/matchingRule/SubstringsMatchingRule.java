/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

import java.util.Map;
import java.util.TreeMap;

/**
 * @author Endi S. Dewata
 */
public class SubstringsMatchingRule {

    public final static String CASE_IGNORE      = "caseIgnoreSubstringsMatch";
    public final static String CASE_EXACT       = "caseExactSubstringsMatch";
    public final static String NUMERIC_STRING   = "numericStringSubstringsMatch";
    public final static String OCTET_STRING     = "octetStringSubstringsMatch";

    public final static SubstringsMatchingRule DEFAULT = new SubstringsMatchingRule();

    public static Map instances = new TreeMap();

    static {
        instances.put(CASE_IGNORE,    DEFAULT);
        instances.put(CASE_EXACT,     DEFAULT);
        instances.put(NUMERIC_STRING, DEFAULT);
        instances.put(OCTET_STRING,   DEFAULT);
    }

    public static SubstringsMatchingRule getInstance(String name) {
        if (name == null) return DEFAULT;

        SubstringsMatchingRule substringsMatchingRule = (SubstringsMatchingRule)instances.get(name);
        if (substringsMatchingRule == null) return DEFAULT;

        return substringsMatchingRule;
    }
}
