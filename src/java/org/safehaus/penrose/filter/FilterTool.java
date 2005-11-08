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
package org.safehaus.penrose.filter;


import java.util.*;
import java.io.StringReader;

import org.apache.log4j.Logger;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.schema.matchingRule.OrderingMatchingRule;
import org.safehaus.penrose.schema.matchingRule.SubstringsMatchingRule;
import org.safehaus.penrose.mapping.*;

/**
 * @author Endi S. Dewata
 */
public class FilterTool {

    Logger log = Logger.getLogger(getClass());

    public FilterContext filterContext;
    public Schema schema;

    public int debug = 0;

    public FilterTool(FilterContext filterContext) throws Exception {
        this.filterContext = filterContext;
        schema = filterContext.getSchema();
    }

    public Filter parseFilter(String filter) throws Exception {
        StringReader in = new StringReader(filter);
        FilterParser parser = new FilterParser(in);
        return parser.parse();
    }

    public boolean checkFilter(Entry sr, Filter filter) throws Exception {
    	log.debug("Checking filter on "+sr.getDn());
        return isValidEntry(sr, filter);
    }

    public boolean isValidEntry(Entry entry, Filter filter) throws Exception {
        //log.debug("Checking filter "+filter);
        boolean result = false;

        if (filter instanceof NotFilter) {
            result = isValidEntry(entry, (NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = isValidEntry(entry, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = isValidEntry(entry, (OrFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            result = isValidEntry(entry, (SubstringFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = isValidEntry(entry, (PresentFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            result = isValidEntry(entry, (SimpleFilter)filter);
        }

        //log.debug(" - "+filter+" -> "+(result ? "ok" : "false"));

        return result;
    }

    public boolean isValidEntry(Entry entry, SubstringFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        Collection substrings = filter.getSubstrings();

        AttributeValues values = entry.getAttributeValues();
        Collection set = values.get(attributeName);
        if (set == null) return false;

        AttributeType attributeType = schema.getAttributeType(attributeName);

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


    public boolean isValidEntry(Entry entry, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        if (attributeName.equalsIgnoreCase("objectclass")) {
            return true;
        } else {
            AttributeValues values = entry.getAttributeValues();
            return values.contains(attributeName);
        }
    }

    public boolean isValidEntry(Entry entry, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        String attributeValue = filter.getValue();

        if (attributeName.equalsIgnoreCase("objectclass")) {
            return entry.getEntryDefinition().containsObjectClass(attributeValue);
        }

        AttributeValues values = entry.getAttributeValues();
        Collection set = values.get(attributeName);
        if (set == null) return false;

        AttributeType attributeType = schema.getAttributeType(attributeName);

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

    public boolean isValidEntry(Entry entry, NotFilter filter) throws Exception {
        Filter f = filter.getFilter();
        boolean result = isValidEntry(entry, f);
        return !result;
    }

    public boolean isValidEntry(Entry entry, AndFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValidEntry(entry, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean isValidEntry(Entry entry, OrFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValidEntry(entry, f);
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
            Row pk = (Row)i.next();

            Filter f = createFilter(pk, includeValues);
            filter = appendOrFilter(filter, f);
        }

        return filter;
    }

    public static Filter createFilter(Row row) {
        return createFilter(row, true);
    }

    public static Filter createFilter(Row row, boolean includeValues) {

        Filter f = null;

        for (Iterator j=row.getNames().iterator(); j.hasNext(); ) {
            String name = (String)j.next();
            Object value = row.get(name);
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

    public boolean isValidEntry(EntryDefinition entryDefinition, Filter filter) throws Exception {
        log.debug("Checking filter "+filter);

        boolean result = false;

        if (filter instanceof NotFilter) {
            result = isValidEntry(entryDefinition, (NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = isValidEntry(entryDefinition, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = isValidEntry(entryDefinition, (OrFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            result = isValidEntry(entryDefinition, (SimpleFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = isValidEntry(entryDefinition, (PresentFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            result = isValidEntry(entryDefinition, (SubstringFilter)filter);
        }

        // log.debug("=> "+filter+" ("+filter.getClass().getName()+"): "+result);

        return result;
    }

    public boolean isValidEntry(EntryDefinition entryDefinition, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        String attributeValue = filter.getValue();

        if (attributeName.equalsIgnoreCase("objectclass") && entryDefinition.containsObjectClass(attributeValue)) return true;

        AttributeDefinition attributeDefinition = entryDefinition.getAttributeDefinition(attributeName);
        if (attributeDefinition == null) return false;

        String value = attributeDefinition.getConstant();
        if (value == null) return true;

        AttributeType attributeType = schema.getAttributeType(attributeName);

        if ("=".equals(operator)) {
            String equality = attributeType == null ? null : attributeType.getEquality();
            EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

            boolean b = equalityMatchingRule.compare(value, attributeValue);
            log.debug(" - ["+value+"] => "+b);

            if (!b) return false;

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

        return true;
    }

    public boolean isValidEntry(EntryDefinition entryDefinition, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectclass")) return true;

        return entryDefinition.getAttributeDefinition(attributeName) != null;
    }

    public boolean isValidEntry(EntryDefinition entryDefinition, SubstringFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        Collection substrings = filter.getSubstrings();

        AttributeDefinition attributeDefinition = entryDefinition.getAttributeDefinition(attributeName);
        if (attributeDefinition == null) return false;

        String value = attributeDefinition.getConstant();
        if (value == null) return true;

        AttributeType attributeType = schema.getAttributeType(attributeName);

        String substring = attributeType == null ? null : attributeType.getSubstring();
        SubstringsMatchingRule substringsMatchingRule = SubstringsMatchingRule.getInstance(substring);

        boolean b = substringsMatchingRule.compare(value, substrings);
        log.debug(" - ["+value+"] => "+b);

        return b;
    }

    public boolean isValidEntry(EntryDefinition entryDefinition, NotFilter filter) throws Exception {
        Filter f = filter.getFilter();
        boolean result = isValidEntry(entryDefinition, f);
        return result;
    }

    public boolean isValidEntry(EntryDefinition entryDefinition, AndFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValidEntry(entryDefinition, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean isValidEntry(EntryDefinition entryDefinition, OrFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValidEntry(entryDefinition, f);
            if (result) return true;
        }
        return false;
    }

    public boolean isValidEntry(AttributeValues attributeValues, Filter filter) throws Exception {
        log.debug("Checking filter "+filter);

        boolean result = false;

        if (filter instanceof NotFilter) {
            result = isValidEntry(attributeValues, (NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = isValidEntry(attributeValues, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = isValidEntry(attributeValues, (OrFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            result = isValidEntry(attributeValues, (SimpleFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = isValidEntry(attributeValues, (PresentFilter)filter);
        }

        // log.debug("=> "+filter+" ("+filter.getClass().getName()+"): "+result);

        return result;
    }

    public boolean isValidEntry(AttributeValues attributeValues, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        String attributeValue = filter.getValue();

        Collection values = attributeValues.get(attributeName);

        for (Iterator i=values.iterator(); i.hasNext(); ) {
            String value = i.next().toString();

            int c = attributeValue.toString().compareTo(value);

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
        }

        return true;
    }

    public boolean isValidEntry(AttributeValues attributeValues, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectclass")) return true;

        return attributeValues.contains(attributeName);
    }

    public boolean isValidEntry(AttributeValues attributeValues, NotFilter filter) throws Exception {
        Filter f = filter.getFilter();
        boolean result = isValidEntry(attributeValues, f);
        return result;
    }

    public boolean isValidEntry(AttributeValues attributeValues, AndFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValidEntry(attributeValues, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean isValidEntry(AttributeValues attributeValues, OrFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValidEntry(attributeValues, f);
            if (result) return true;
        }
        return false;
    }
}
