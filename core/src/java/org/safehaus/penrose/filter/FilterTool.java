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

import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.schema.matchingRule.OrderingMatchingRule;
import org.safehaus.penrose.schema.matchingRule.SubstringsMatchingRule;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.entry.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class FilterTool {

    public static Logger log = LoggerFactory.getLogger(FilterTool.class);

    private SchemaManager schemaManager;

    public FilterTool() throws Exception {
    }

    public static Filter parseFilter(String filter) throws Exception {
        StringReader in = new StringReader(filter);
        FilterParser parser = new FilterParser(in);
        return parser.parse();
    }

    public boolean isValid(Entry entry, Filter filter) throws Exception {
        return isValid(entry.getAttributes(), filter);
    }

    public boolean isValid(Attributes attributes, Filter filter) throws Exception {

        //log.debug("Checking filter "+filter);
        boolean result = false;

        if (filter == null) {
            result = true;

        } else if (filter instanceof NotFilter) {
            result = isValid(attributes, (NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = isValid(attributes, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = isValid(attributes, (OrFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            result = isValid(attributes, (SubstringFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = isValid(attributes, (PresentFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            result = isValid(attributes, (SimpleFilter)filter);
        }

        //log.debug(" - "+filter+" -> "+(result ? "ok" : "false"));

        return result;
    }

    public boolean isValid(Attributes attributes, SubstringFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        Collection substrings = filter.getSubstrings();

        Attribute attribute = attributes.get(attributeName);
        if (attribute == null) return false;

        Collection set = attribute.getValues();

        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String substring = attributeType == null ? null : attributeType.getSubstring();
        SubstringsMatchingRule substringsMatchingRule = SubstringsMatchingRule.getInstance(substring);

        for (Iterator i=set.iterator(); i.hasNext(); ) {
            String value = i.next().toString();

            boolean b = substringsMatchingRule.compare(value, substrings);
            log.debug(" - ["+value+"] => "+b);

            if (b) return true;
        }

        return false;
    }


    public boolean isValid(Attributes attributes, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        if (attributeName.equalsIgnoreCase("objectclass")) {
            return true;
        } else {
            return attributes.get(attributeName) != null;
        }
    }

    public boolean isValid(Attributes attributes, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        String attributeValue = filter.getValue();

        Attribute attribute = attributes.get(attributeName);
        if (attribute == null) return false;

        Collection set = attribute.getValues();
        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        if ("=".equals(operator)) {
            String equality = attributeType == null ? null : attributeType.getEquality();
            EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

            for (Iterator i=set.iterator(); i.hasNext(); ) {
                String value = i.next().toString();

                boolean b = equalityMatchingRule.compare(value, attributeValue);
                //log.debug(" - ["+value+"] => "+b);

                if (b) return true;
            }

        } else if ("<=".equals(operator) || ">=".equals(operator)) {
            String ordering = attributeType == null ? null : attributeType.getOrdering();
            OrderingMatchingRule orderingMatchingRule = OrderingMatchingRule.getInstance(ordering);

            for (Iterator i=set.iterator(); i.hasNext(); ) {
                String value = i.next().toString();

                int c = orderingMatchingRule.compare(value, attributeValue);
                log.debug(" - ["+value+"] => "+c);

                if ("<=".equals(operator) && c <= 0) return true;
                if (">=".equals(operator) && c >= 0) return true;
            }

        } else {
            throw new Exception("Unsupported operator \""+operator+"\" in \""+filter+"\"");
        }

        return false;
    }

    public boolean isValid(Attributes attributes, NotFilter filter) throws Exception {
        Filter f = filter.getFilter();
        boolean result = isValid(attributes, f);
        return !result;
    }

    public boolean isValid(Attributes attributes, AndFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValid(attributes, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean isValid(Attributes attributes, OrFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValid(attributes, f);
            if (result) return true;
        }
        return false;
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

    public static Filter createFilter(Collection keys) {
        return createFilter(keys, true);
    }

    public static Filter createFilter(Collection keys, boolean includeValues) {

        Filter filter = null;

        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            RDN pk = (RDN)i.next();

            Filter f = createFilter(pk, includeValues);
            filter = appendOrFilter(filter, f);
        }

        return filter;
    }

    public static Filter createFilter(RDN rdn) {
        return createFilter(rdn, true);
    }

    public static Filter createFilter(RDN rdn, boolean includeValues) {

        Filter f = null;

        for (Iterator i =rdn.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = rdn.get(name);
            if (value == null) continue;

            String strVal;
            if (includeValues) {
                strVal = value == null ? null : value.toString();
            } else {
                strVal = "?";
            }

            SimpleFilter sf = new SimpleFilter(name, "=", strVal);
            f = appendAndFilter(f, sf);
        }

        return f;
    }

    public static Filter appendAndFilter(Filter filter, Filter newFilter) {
        if (newFilter == null || newFilter.equals(filter)) {
            // ignore

        } else if (filter == null) {
            filter = newFilter;

        } else if (filter instanceof AndFilter) {
            AndFilter af = (AndFilter)filter;
            if (newFilter instanceof AndFilter) {
                for (Iterator i=((AndFilter)newFilter).getFilters().iterator(); i.hasNext(); ) {
                    Filter f = (Filter)i.next();
                    if (!af.containsFilter(f)) af.addFilter(f);
                }
            } else {
                if (!af.containsFilter(newFilter)) af.addFilter(newFilter);
            }

        } else {
            AndFilter af = new AndFilter();
            af.addFilter(filter);
            af.addFilter(newFilter);
            filter = af;
        }

        return filter;
    }

    public static Filter appendOrFilter(Filter filter, Filter newFilter) {
        if (newFilter == null || newFilter.equals(filter)) {
            // ignore

        } else if (filter == null) {
            filter = newFilter;

        } else if (filter instanceof OrFilter) {
            OrFilter of = (OrFilter)filter;
            if (newFilter instanceof OrFilter) {
                for (Iterator i=((OrFilter)newFilter).getFilters().iterator(); i.hasNext(); ) {
                    Filter f = (Filter)i.next();
                    if (!of.containsFilter(f)) of.addFilter(f);
                }
            } else {
                if (!of.containsFilter(newFilter)) of.addFilter(newFilter);
            }

        } else {
            OrFilter of = new OrFilter();
            of.addFilter(filter);
            of.addFilter(newFilter);
            filter = of;
        }

        return filter;
    }

    public boolean isValid(EntryMapping entryMapping, Filter filter) throws Exception {
        log.debug("Checking filter "+filter);

        boolean result = false;

        if (filter == null) {
            result = true;

        } else if (filter instanceof NotFilter) {
            result = isValid(entryMapping, (NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = isValid(entryMapping, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = isValid(entryMapping, (OrFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            result = isValid(entryMapping, (SimpleFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = isValid(entryMapping, (PresentFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            result = isValid(entryMapping, (SubstringFilter)filter);
        }

        // log.debug("=> "+filter+" ("+filter.getClass().getName()+"): "+result);

        return result;
    }

    public boolean isValid(EntryMapping entryMapping, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        String attributeValue = filter.getValue();

        if (attributeName.equalsIgnoreCase("objectclass") && entryMapping.containsObjectClass(attributeValue)) return true;

        AttributeMapping attributeMapping = entryMapping.getAttributeMapping(attributeName);
        if (attributeMapping == null) return false;

        Object value = attributeMapping.getConstant();
        if (value == null) return true;

        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        if ("=".equals(operator)) {
            String equality = attributeType == null ? null : attributeType.getEquality();
            EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

            boolean b = equalityMatchingRule.compare(value, attributeValue);
            log.debug(" - ["+value+"] => "+b);

            if (b) return true;

        } else if ("<=".equals(operator) || ">=".equals(operator)) {
            String ordering = attributeType == null ? null : attributeType.getOrdering();
            OrderingMatchingRule orderingMatchingRule = OrderingMatchingRule.getInstance(ordering);

            int c = orderingMatchingRule.compare(value, attributeValue);
            log.debug(" - ["+value+"] => "+c);

            if ("<=".equals(operator) && c > 0) return true;
            if (">=".equals(operator) && c < 0) return true;

        } else {
            throw new Exception("Unsupported operator \""+operator+"\" in \""+filter+"\"");
        }

        return false;
    }

    public boolean isValid(EntryMapping entryMapping, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectclass")) return true;

        return entryMapping.getAttributeMapping(attributeName) != null;
    }

    public boolean isValid(EntryMapping entryMapping, SubstringFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        Collection substrings = filter.getSubstrings();

        AttributeMapping attributeMapping = entryMapping.getAttributeMapping(attributeName);
        if (attributeMapping == null) return false;

        Object value = attributeMapping.getConstant();
        if (value == null) return true;

        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String substring = attributeType == null ? null : attributeType.getSubstring();
        SubstringsMatchingRule substringsMatchingRule = SubstringsMatchingRule.getInstance(substring);

        boolean b = substringsMatchingRule.compare(value, substrings);
        log.debug(" - ["+value+"] => "+b);

        if (b) return true;

        return false;
    }

    public boolean isValid(EntryMapping entryMapping, NotFilter filter) throws Exception {
        Filter f = filter.getFilter();
        boolean result = isValid(entryMapping, f);
        return result;
    }

    public boolean isValid(EntryMapping entryMapping, AndFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValid(entryMapping, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean isValid(EntryMapping entryMapping, OrFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValid(entryMapping, f);
            if (result) return true;
        }
        return false;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public static boolean isValid(RDN rdn, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        String attributeValue = filter.getValue();

        Object value = rdn.get(attributeName);
        if (value == null) return false;

        int c = attributeValue.toString().compareTo(value.toString());

        if ("=".equals(operator)) {
            if (c != 0) return false;

        } else if ("<".equals(operator)) {
            if (c >= 0) return false;

        } else if ("<=".equals(operator)) {
            if (c > 0) return false;

        } else if (">".equals(operator)) {
            if (c <= 0) return false;

        } else if (">=".equals(operator)) {
            if (c < 0) return false;

        } else {
            throw new Exception("Unsupported operator \""+operator+"\" in \""+filter+"\"");
        }

        return true;
    }

    public static boolean isValid(RDN rdn, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectclass")) return true;

        return rdn.contains(attributeName);
    }

    public static boolean isValid(RDN rdn, AndFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValid(rdn, f);
            if (!result) return false;
        }
        return true;
    }

    public static boolean isValid(RDN rdn, OrFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValid(rdn, f);
            if (result) return true;
        }
        return false;
    }

    public static boolean isValid(RDN rdn, NotFilter filter) throws Exception {
        Filter f = filter.getFilter();
        return isValid(rdn, f);
    }

    public static boolean isValid(RDN rdn, Filter filter) throws Exception {
        log.debug("Checking filter "+filter);

        boolean result = false;

        if (filter == null) {
            result = true;

        } else if (filter instanceof NotFilter) {
            result = isValid(rdn, (NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = isValid(rdn, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = isValid(rdn, (OrFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            result = isValid(rdn, (SimpleFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = isValid(rdn, (PresentFilter)filter);
        }

        // log.debug("=> "+filter+" ("+filter.getClass().getName()+"): "+result);

        return result;
    }

    public static String escape(String value) {

        StringBuilder sb = new StringBuilder();
        char chars[] = value.toCharArray();

        for (int i=0; i<chars.length; i++) {
            char c = chars[i];

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
