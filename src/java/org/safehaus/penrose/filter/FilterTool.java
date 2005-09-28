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

    public boolean isValidEntry(Entry sr, Filter filter) throws Exception {
        //penrose.log.filter.debug(filter.getClass().getName()+": "+filter);
        boolean result = false;

        if (filter instanceof SimpleFilter) {
            result = isValidEntry(sr, (SimpleFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            result = isValidEntry(sr, (SubstringFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = isValidEntry(sr, (PresentFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = isValidEntry(sr, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = isValidEntry(sr, (OrFilter)filter);
        }

        log.debug(" - "+filter+" -> "+(result ? "ok" : "false"));

        return result;
    }

    public boolean isValidEntry(Entry sr, SubstringFilter filter) throws Exception {
        String attributeName = filter.getAttr();
        List substrings = filter.getSubstrings();
        return true;
    }

    public boolean isValidEntry(Entry sr, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttr();
        if (attributeName.toLowerCase().equals("objectclass")) {
            return true;
        } else {
            AttributeValues values = sr.getAttributeValues();
            return values.contains(attributeName);
        }
    }

    public boolean isValidEntry(Entry sr, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String attributeComparison = filter.getValue();

        if (attributeName.toLowerCase().equals("objectclass")) {
            return sr.getEntryDefinition().getObjectClasses().contains(attributeComparison);
        }

        AttributeValues values = sr.getAttributeValues();
        Collection set = values.get(attributeName);
        if (set == null) return false;

        for (Iterator i=set.iterator(); i.hasNext(); ) {
            String value = (String)i.next();
            if (attributeComparison.equalsIgnoreCase(value)) return true;
        }
        return false;
    }

    public boolean isValidEntry(Entry sr, AndFilter filter) throws Exception {
        for (Iterator i=filter.getFilterList().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValidEntry(sr, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean isValidEntry(Entry sr, OrFilter filter) throws Exception {
        for (Iterator i=filter.getFilterList().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = isValidEntry(sr, f);
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

            if (filter == null) {
                filter = f;

            } else if (!(filter instanceof OrFilter)) {
                OrFilter of = new OrFilter();
                of.addFilterList(filter);
                of.addFilterList(f);
                filter = of;

            } else {
                OrFilter of = (OrFilter)filter;
                of.addFilterList(f);
            }
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

            if (f == null) {
                f = sf;

            } else if (!(f instanceof AndFilter)) {
                AndFilter af = new AndFilter();
                af.addFilterList(f);
                af.addFilterList(sf);
                f = af;

            } else {
                AndFilter af = (AndFilter)f;
                af.addFilterList(sf);
            }
        }

        return f;
    }

}
