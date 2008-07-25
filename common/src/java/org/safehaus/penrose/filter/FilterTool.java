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
import java.io.UnsupportedEncodingException;

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

    public static String escape(Object value) {
        StringBuilder sb = new StringBuilder();

        if (value == null) {
            // empty string
            
        } else if (value instanceof byte[]) {
            for (byte b : (byte[])value) {
                int i = 0xff & b;
                String hex = Integer.toHexString(i);
                sb.append('\\');
                if (hex.length() < 2) sb.append('0');
                sb.append(hex);
            }

        } else {
            byte[] bytes;
            try {
                bytes = value.toString().getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage(), e);
            }

            for (byte b : bytes) {
                int c = 0xff & b;
                if (c == '*' || c == '(' || c == ')' || c == '\\') {
                    String hex = Integer.toHexString(c);
                    sb.append('\\');
                    if (hex.length() < 2) sb.append('0');
                    sb.append(hex);

                } else if (Character.isISOControl(c) || c > 0x7f) {
                    String hex = Integer.toHexString(c);
                    sb.append('\\');
                    if (hex.length() < 2) sb.append('0');
                    sb.append(hex);

                } else {
                    sb.append((char)b);
                }
            }
/*
            for (char c : value.toString().toCharArray()) {
                if (c == '*' || c == '(' || c == ')' || c == '\\') {
                    String hex = Integer.toHexString(c);
                    sb.append('\\');
                    if (hex.length() < 2) sb.append('0');
                    sb.append(hex);

                } else {
                    sb.append(c);
                }
            }
*/
        }


        return sb.toString();
    }

    public static Object unescape(String value) {

        StringBuilder sb = new StringBuilder();
        char chars[] = value.toCharArray();
        int start = 0;
        int end = chars.length;

        for (int i=start; i<end; i++) {
            char c = chars[i];

            if (c == '\\') {
                
                if (isHexDigit(chars[i+1]) && isHexDigit(chars[i+2])) {

                    int counter = 1;
                    int s = i; // points to slash
                    i += 3;
                    while(i < end-2
                            && chars[i] == '\\'
                            && isHexDigit(chars[i+1])
                            && isHexDigit(chars[i+2])) {
                        counter++;
                        i += 3;
                    }

                    byte bytes[] = new byte[counter];

                    i = s;
                    for(int j = 0; j < counter; j++) {
                        String x = new String(new char[] { chars[i+1], chars[i+2] });
                        bytes[j] = (byte)Integer.parseInt(x, 16);
                        i += 3;
                    }

                    String str;
                    try {
                        str = new String(bytes, "UTF-8");

                    } catch (Exception e) {
                        //throw new RuntimeException(e.getMessage(), e);
                        return bytes;
                    }

                    sb.append(str);

                    i--;

                } else {
                    i++;
                     sb.append(chars[i]);
                 }

            } else {
                sb.append(c);
            }
        }

        //String s = sb.toString();
        //log.debug("Unescaped ["+value+"] to ["+s+"]");
        //return s;

        return sb.toString();
    }

    private static boolean isHexDigit(char c) {
		return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
	}
}
