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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.EntryDefinition;

/**
 * @author Endi S. Dewata
 */
public class FilterTool {

    Logger log = LoggerFactory.getLogger(getClass());

    public Penrose penrose;
    public int debug = 0;

    public FilterTool(Penrose penrose) {
        this.penrose = penrose;
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
        //penrose.log.filter.debug(filter.getClass().getName()+": "+filter);
        boolean result = false;

        if (filter instanceof SimpleFilter) {
            result = isValidEntry(entry, (SimpleFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            result = isValidEntry(entry, (SubstringFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = isValidEntry(entry, (PresentFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = isValidEntry(entry, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = isValidEntry(entry, (OrFilter)filter);
        }

        log.debug(" - "+filter+" -> "+(result ? "ok" : "false"));

        return result;
    }

    public boolean isValidEntry(Entry entry, SubstringFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        Collection substrings = filter.getSubstrings();
        return true;
    }

    public boolean isValidEntry(Entry entry, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        if (attributeName.toLowerCase().equals("objectclass")) {
            return true;
        } else {
            AttributeValues values = entry.getAttributeValues();
            return values.contains(attributeName);
        }
    }

    public boolean isValidEntry(Entry entry, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String attributeComparison = filter.getValue();

        if (attributeName.toLowerCase().equals("objectclass")) {
            return entry.getEntryDefinition().getObjectClasses().contains(attributeComparison);
        }

        AttributeValues values = entry.getAttributeValues();
        Collection set = values.get(attributeName);
        if (set == null) return false;

        for (Iterator i=set.iterator(); i.hasNext(); ) {
            Object value = i.next();
            if (attributeComparison.equalsIgnoreCase(value.toString())) return true;
        }
        return false;
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

    public Filter createFilter(Collection keys) {
        return createFilter(keys, true);
    }

    public Filter createFilter(Collection keys, boolean includeValues) {

        Filter filter = null;

        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            Filter f = createFilter(pk, includeValues);
            filter = appendOrFilter(filter, f);
        }

        return filter;
    }

    public Filter createFilter(Row values, boolean includeValues) {

        Filter f = null;

        for (Iterator j=values.getNames().iterator(); j.hasNext(); ) {
            String name = (String)j.next();
            Object value = values.get(name);
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

    public Filter appendAndFilter(Filter filter, Filter newFilter) {
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

    public Filter appendOrFilter(Filter filter, Filter newFilter) {
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
        boolean result = false;

        if (filter instanceof SimpleFilter) {
            result = isValidEntry(entryDefinition, (SimpleFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = isValidEntry(entryDefinition, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = isValidEntry(entryDefinition, (OrFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = isValidEntry(entryDefinition, (PresentFilter)filter);
        }

        log.debug("=> "+filter+" ("+filter.getClass().getName()+"): "+result);

        return result;
    }

    public boolean isValidEntry(EntryDefinition entryDefinition, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();

        if (attributeName.toLowerCase().equals("objectclass")) return true;

        return entryDefinition.getAttributeDefinition(attributeName) != null;
    }

    public boolean isValidEntry(EntryDefinition entryDefinition, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();

        if (attributeName.toLowerCase().equals("objectclass")) return true;

        return entryDefinition.getAttributeDefinition(attributeName) != null;
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

}
