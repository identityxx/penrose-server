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
package org.safehaus.penrose.filter;

import java.util.*;
import java.io.StringReader;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class FilterTool {

    public static Logger log = LoggerFactory.getLogger(FilterTool.class);

    public static Filter parseFilter(String filter) throws Exception {
        if (filter == null || "".equals(filter)) return null;
        StringReader in = new StringReader(filter);
        FilterParser parser = new FilterParser(in);
        return parser.parse();
    }

    public boolean containsAttribute(List filterTree, String attributeName) throws Exception {
        if (filterTree == null) return false;

        Object object = filterTree.get(0);

        log.debug("operator: "+object+" ("+object.getClass().getName()+")");
        String operator = (String)object;

        if (operator.equals("=")) {
            String lhs = (String)filterTree.get(1);
            String rhs = (String)filterTree.get(2);

            if (lhs.equals(attributeName)) return true;
            if (rhs.equals(attributeName)) return true;

        } else if (operator.equals("&")) {
            for (int i=1; i<filterTree.size(); i++) {
                Vector exp = (Vector)filterTree.get(i);
                if (containsAttribute(exp, attributeName)) return true;
            }
            return false;

        } else if (operator.equals("|")) {
            for (int i=1; i<filterTree.size(); i++) {
                Vector exp = (Vector)filterTree.get(i);
                if (containsAttribute(exp, attributeName)) return true;
            }
            return false;
        }

        return false;
    }

    public static Filter appendAndFilter(Filter filter, Filter newFilter) {
        if (filter == null) {
            return newFilter;

        } else if (newFilter == null || newFilter.equals(filter)) {
            return filter;

        } else if (filter instanceof AndFilter) {
            AndFilter af = (AndFilter)filter;
            if (newFilter instanceof AndFilter) {
                for (Filter f : ((AndFilter) newFilter).getFilters()) {
                    if (!af.contains(f)) af.addFilter(f);
                }
            } else {
                if (!af.contains(newFilter)) af.addFilter(newFilter);
            }
            return af;

        } else {
            AndFilter af = new AndFilter();
            af.addFilter(filter);
            af.addFilter(newFilter);
            return af;
        }
    }

    public static Filter appendOrFilter(Filter filter, Filter newFilter) {
        if (filter == null) {
            return newFilter;

        } else if (newFilter == null || newFilter.equals(filter)) {
            return filter;

        } else if (filter instanceof OrFilter) {
            OrFilter of = (OrFilter)filter;
            if (newFilter instanceof OrFilter) {
                for (Filter f : ((OrFilter) newFilter).getFilters()) {
                    if (!of.contains(f)) of.addFilter(f);
                }
            } else {
                if (!of.contains(newFilter)) of.addFilter(newFilter);
            }
            return of;

        } else {
            OrFilter of = new OrFilter();
            of.addFilter(filter);
            of.addFilter(newFilter);
            return of;
        }
    }

    public static String escape(String value) {

        StringBuilder sb = new StringBuilder();
        char chars[] = value.toCharArray();

        for (char c : chars) {
            if (c == '*' || c == '(' || c == ')' || c == '\\') {
                String hex = Integer.toHexString(c);
                sb.append('\\');
                if (hex.length() < 2) sb.append('0');
                sb.append(hex);

            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }

    public static String unescape(String value) {

        StringBuilder sb = new StringBuilder();
        char chars[] = value.toCharArray();

        for (int i=0; i<chars.length; i++) {
            char c = chars[i];

            if (c == '\\') {
                int h1 = chars[++i];
                if (h1 >= '0' && h1 <= '9') {
                    h1 = h1 - '0';
                } else if (h1 >= 'a' && h1 <= 'f') {
                    h1 = h1 - 'a' + 10;
                } else { // 'A' - 'F'
                    h1 = h1 - 'A' + 10;
                }

                int h0 = chars[++i];
                if (h0 >= '0' && h0 <= '9') {
                    h0 = h0 - '0';
                } else if (h0 >= 'a' && h0 <= 'f') {
                    h0 = h0 - 'a' + 10;
                } else { // 'A' - 'F'
                    h0 = h0 - 'A' + 10;
                }

                int dec = h1 * 16 + h0;
                sb.append((char)dec);

            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }
}
