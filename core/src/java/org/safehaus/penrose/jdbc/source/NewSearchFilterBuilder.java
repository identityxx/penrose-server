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
package org.safehaus.penrose.jdbc.source;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.source.Field;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class NewSearchFilterBuilder {

    public Logger log = LoggerFactory.getLogger(getClass());

    JDBCJoinSource joinSource;

    Interpreter interpreter;

    Map<String,String> originalAliases = new LinkedHashMap<String,String>(); // need to maintain order
    Map<String,String> newAliases = new HashMap<String,String>();

    Filter sourceFilter;

    public NewSearchFilterBuilder(
            JDBCJoinSource joinSource
    ) throws Exception {
        this.joinSource = joinSource;
    }

    public Filter convert(
            Filter filter
    ) throws Exception {

        if (filter instanceof NotFilter) {
            return convert((NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            return convert((AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            return convert((OrFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            return convert((SimpleFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            return convert((SubstringFilter)filter);

        } else if (filter instanceof PresentFilter) {
            return convert((PresentFilter)filter);
        }

        return null;
    }

    public Filter convert(NotFilter filter) throws Exception {
        Filter newFilter = convert(filter.getFilter());
        return new NotFilter(newFilter);
    }

    public Filter convert(AndFilter filter) throws Exception {

        Filter newFilter = null;

        Collection<Filter> filters = filter.getFilters();
        for (Filter f : filters) {
            Filter nf = convert(f);
            newFilter = FilterTool.appendAndFilter(newFilter, nf);
        }

        return newFilter;
    }

    public Filter convert(OrFilter filter) throws Exception {

        Filter newFilter = null;

        Collection<Filter> filters = filter.getFilters();
        for (Filter f : filters) {
            Filter nf = convert(f);
            newFilter = FilterTool.appendOrFilter(newFilter, nf);
        }

        return newFilter;
    }

    public Filter convert(
            SimpleFilter filter
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Converting filter "+filter+".");

        String attributeName  = filter.getAttribute();
        String operator       = filter.getOperator();
        Object attributeValue = filter.getValue();

        Field field = joinSource.getField(attributeName);
        if (field == null) return null;

        String variable = field.getVariable();
        if (variable == null) return null;

        int i = variable.indexOf('.');
        if (i < 0) return null;

        String alias = variable.substring(0, i);
        String column = variable.substring(i+1);

        String newAlias = createNewAlias(alias);

        Filter newFilter = new SimpleFilter(newAlias+"."+column, operator, attributeValue);
        if (debug) log.debug("New filter: "+newFilter);

        return newFilter;
    }

    public Filter convert(
            SubstringFilter filter
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Converting filter "+filter+".");

        String attributeName = filter.getAttribute();
        Collection<Object> substrings = filter.getSubstrings();

        Field field = joinSource.getField(attributeName);
        if (field == null) return null;

        String variable = field.getVariable();
        if (variable == null) return null;

        int i = variable.indexOf('.');
        if (i < 0) return null;

        String alias = variable.substring(0, i);
        String column = variable.substring(i+1);

        String newAlias = createNewAlias(alias);

        StringBuilder sb = new StringBuilder();
        for (Object o : substrings) {
            if (o.equals(SubstringFilter.STAR)) {
                sb.append("%");
            } else {
                String substring = (String) o;
                sb.append(substring);
            }
        }

        String value = sb.toString();

        Filter newFilter = new SimpleFilter(newAlias+"."+column, "like", value);
        if (debug) log.debug("New filter: "+newFilter);

        return newFilter;
    }

    public Filter convert(
            PresentFilter filter
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Converting filter "+filter+".");

        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectClass")) return null;

        Field field = joinSource.getField(attributeName);
        if (field == null) return null;

        String variable = field.getVariable();
        if (variable == null) return null;

        int i = variable.indexOf('.');
        if (i < 0) return null;

        String alias = variable.substring(0, i);
        String column = variable.substring(i+1);

        String newAlias = createNewAlias(alias);

        Filter newFilter = new PresentFilter(newAlias+"."+column);
        if (debug) log.debug("New filter: "+newFilter);

        return newFilter;
    }

    public void append(Filter filter) throws Exception {
        sourceFilter = FilterTool.appendAndFilter(sourceFilter, filter);
    }

    public Filter getFilter() {
        return sourceFilter;
    }

    public String createNewAlias(String alias) {

        if (joinSource.isPrimarySourceAlias(alias)) return alias;

        String newAlias = newAliases.get(alias);
        if (newAlias != null) return newAlias;

        int counter = 2;
        newAlias = alias+counter;

        while (joinSource.getSource(newAlias) != null) {
            counter++;
            newAlias = alias+counter;
        }

        originalAliases.put(newAlias, alias);
        newAliases.put(alias, newAlias);

        return newAlias;
    }

    public void setTableAlias(String alias, String newAlias) {
        if (joinSource.isPrimarySourceAlias(alias)) return;

        originalAliases.put(newAlias, alias);
    }

    public Collection<String> getNewAliases() {
        return originalAliases.keySet();
    }

    public String getOriginalAlias(String newAlias) {
        return originalAliases.get(newAlias);
    }
}