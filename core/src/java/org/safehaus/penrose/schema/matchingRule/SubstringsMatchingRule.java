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
import org.safehaus.penrose.filter.SubstringFilter;

import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * @author Endi S. Dewata
 */
public class SubstringsMatchingRule {

    Logger log = LoggerFactory.getLogger(getClass());

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

    public boolean compare(Object object, Collection substrings) {
        log.debug("comparing ["+object+"] with "+substrings);
        if (object == null) return false;

        StringBuilder sb = new StringBuilder();
        sb.append("^");
        for (Iterator i=substrings.iterator(); i.hasNext(); ) {
            Object o = i.next();
            if (o.equals(SubstringFilter.STAR)) {
                sb.append(".*");
            } else {
                String substring = (String)o;
                sb.append(escape(substring));
            }
        }
        sb.append("$");

        Pattern pattern = Pattern.compile(sb.toString(), Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(object.toString());

        return matcher.find();
    }

    public static String escape(String s) {
        StringBuilder sb = new StringBuilder(s);
        int i = 0;
        while (i<sb.length()) {
            char c = sb.charAt(i);
            if (c == '\\' || c == '.' || c == '?' || c == '*'
                    || c == '^' || c == '$' || c == '{' || c == '}'
                    || c == '[' || c == ']' || c == '(' || c == ')') {
                sb.replace(i, i+1, "\\"+c);
                i += 2;
                continue;
            }
            i++;
        }
        return sb.toString();
    }
}
